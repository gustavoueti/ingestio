# Spec: choose the reader and the writer from the job's configuration

- **Status:** validated
- **Slug:** `config_connectors`. The spec is `specs/config_connectors.md` and its tests are
  `tests/test_config_connectors.py`.
- **Related:** [backlog idea 11](../docs/backlog.md#11-choose-the-reader-and-the-writer-from-the-jobs-configuration),
  [philosophy](../docs/philosophy.md),
  [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0004](../docs/decisions/0004-shared-process-state.md),
  [ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md),
  [specs/append_only_callback.md](append_only_callback.md),
  [specs/scd2_callback.md](scd2_callback.md), and the connector specs
  [postgres](postgres_connectors.md), [disk](disk_connectors.md), [s3](s3_connectors.md),
  [gcs](gcs_connectors.md), [volume](volume_connectors.md),
  [unity_catalog](unity_catalog_connectors.md), [bigquery](bigquery_connectors.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

`ReadSource` and `WriteTarget` stop taking a connector. Each takes the path of the job's JSON
configuration file, reads it, and builds the connector that the file names: `ReadSource` the
reader of the `source` section's `system`, `WriteTarget` the writer of the `target` section's
`system`, with the section's other keys as the connector's constructor arguments of the same
names. Values that do not belong in a JSON file (a `PostgresConnection`, an SDK client, the run's
`watermark_value`, a Spark session) are arguments of the callback, which hands them to the
connector. Only the connectors the library ships can be named, through a fixed, closed table.

This is a breaking change of the public API, and it changes the philosophy: the closed table is
a registry, and connectors are no longer extended by users. A new ADR, 0015, records it, and the
philosophy, ADR 0003 and ADR 0013 are amended (see [ADRs](#adrs)).

The maintainer answered every question of the first draft on 2026-10-23; the answers are
decisions 6 to 17, and choices that rest on one of them name it.

## Motivation

The maintainer's request, translated: "I want the jobs to only `import ingestio` and from there
use the callbacks. Which read connector (source system)? Which write connector (target system)?
That goes in the config: the callback that does the extraction reads it, and the callback that
writes reads which system it writes to, so it uses the right connector."

Today a job does that work itself, as the README's "Structuring a job" shows:

- `main.py` reads `config.json`, because the library never reads it
  ([ADR 0013](../docs/decisions/0013-connector-configuration-convention.md): "Mapping
  configuration keys to arguments is the job's task").
- It imports the connector classes from `ingestio.connector.all`, builds them with
  `config["table_name"]` and the like, and injects them into `ReadSource` and `WriteTarget`.
- Changing the source from disk to S3 means changing the code of `main.py`, not only its
  configuration.
- One flat configuration holds the parameters of the ingestion, so `table_name` names the
  source, and a job that writes to another table must not pass it to the writer. ADR 0013
  records this ambiguity for seven writers and leaves it unresolved.

With this change, `main.py` places the callbacks and passes them the path of the file and the
values that only code can produce. The configuration names the systems, and has one section
for the source and one for the target, which ends the ambiguity.

## Fit with the philosophy

- **Concept: callback.** The choice of the connector belongs to the read and write callbacks:
  they read the configuration, look the system up in a table of the connectors the library
  ships, and build the connector. The connectors do not change and keep deciding nothing: no
  connector reads a file, an environment variable or a secret store, and none chooses a write
  strategy. The configuration file is an argument of the callbacks, like `into` and `key`, and
  the table is part of their implementation, private to the library: neither is a third
  concept. `PostgresConnection` stays a value, as its section of the connectors page says.
- **What conflicts with the philosophy, and how it is resolved.** The maintainer's decisions
  ([Decisions](#decisions), 1 to 5) contradict the current text in four places. Under the
  hierarchy of truth, a spec that contradicts the philosophy is wrong unless an ADR changes the
  philosophy, so this spec comes with ADR 0015 and the amendments listed under [ADRs](#adrs):
  1. **"No internal frameworks, registries or magic"** (Simplicity) and **"no registries"**
     (The user's autonomy). Turning `"postgres"` in a file into `PostgresReader` is a registry,
     even as a fixed dictionary. Decision 4 makes it closed: it holds only the connectors the
     library ships, nothing outside the library can add to it, and there is no mapping
     argument, import path or entry point. The philosophy is amended to allow exactly this
     form, and no other registry.
  2. **"Extending connectors and callbacks must be as simple as possible"** (The user's
     autonomy). Under decision 4, users no longer extend connectors: a system the library does
     not integrate gets a connector contributed to the library, or a feature request. Users
     still write their own callbacks by subclassing `Callback`, with nothing to register.
  3. **ADR 0013.** The job no longer maps configuration keys to constructor arguments, and the
     library now reads a configuration file. What stays: a connector takes an ingestion's
     parameters as constructor arguments named after the configuration keys, has no
     `from_config` and reads no file; credentials never live in the configuration (decision 3).
  4. **ADR 0003.** "A connector is injected into the callback that needs it" no longer holds for
     the library's read and write callbacks, and "a connector written in user code needs no
     import from ingestio" loses its purpose, because no shipped callback accepts one. The
     protocols stay the contract that every shipped connector satisfies (decision 8).
- **Compose, do not impose.** The job still composes its process in code: the steps, their
  order and its own callbacks. What the library now imposes, by the maintainer's decisions 1, 2
  and 4, is that a job which reads or writes through `ReadSource` and `WriteTarget` has a JSON
  configuration file and uses a shipped connector. The README's roadmap calls recipes "an
  optional declarative way to assemble a process, never a required one"; this configuration
  declares connectors, not a process, but it is required for the two callbacks.
- **Essential by default.** `ReadSource` and `WriteTarget` declare no criticality, so they stay
  essential ([ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md)).
  Every error propagates. Configuration errors are raised when the callback is built, before
  anything runs (decision 10).
- **Simplicity.** One private table, one routine that reads the file and checks one section, and
  the two callbacks. No plugin mechanism, no import strings, no conversion of values: the keys
  are the constructor arguments, by name.
- **No dependencies in the core.** The callbacks use `json`, `os` and `pathlib`. The extras stay
  as they are (decision 5), and a configuration that names a system whose extra is missing gets
  the connector's own `ImportError` ([ADR 0002](../docs/decisions/0002-zero-dependency-core.md)
  unchanged). `ingestio` still imports without pyspark or any extra.
- **Streaming.** Nothing here closes the door. The `Reader` and `Writer` protocols and their open
  payload do not change. A streaming connector would be one more shipped system in the table,
  with keys of its own, such as a checkpoint location. See [ADRs](#adrs).

## Scope

In scope:

- the new signatures of `ReadSource` and `WriteTarget`, and the removal of the form that takes
  a connector;
- reading and checking the configuration file: its shape, the `source` and `target` sections,
  the key that names the system, and the JSON types of the values;
- the closed table of the shipped connectors, and for each one its system name, its
  configuration keys and the callback arguments it takes;
- append and replace in `WriteTarget` (decision 13);
- every error, and when it is raised;
- a system whose extra is not installed;
- ADR 0015, and the amendments of the philosophy, ADR 0003 and ADR 0013;
- what the draft specs `append_only_callback` and `scd2_callback` must change, stated here and
  made in their own planning;
- the tests and criteria of the form that takes a connector, which this spec supersedes
  (decision 17).

Out of scope:

- any change to a connector: its arguments, validation, behaviour and messages stay as their
  specs define them;
- new connectors and new extras;
- reading secrets, environment variables, `dbutils` or files other than the configuration; the
  host and the database of PostgreSQL in the configuration (decision 3: the job builds the
  `PostgresConnection`);
- connectors written by the job: a mapping argument, an import path in the configuration, entry
  points, registration (decision 4);
- configuration formats other than JSON (YAML, TOML), comments, includes, interpolation of
  environment variables, a JSON Schema of the file;
- computing, storing or advancing the watermark, and reading it from `process.state`;
- recipes: a configuration that declares steps or callbacks;
- the parameters of the job's own callbacks and of the append-only and SCD2 callbacks;
- several sources or several targets in one file (decision 15);
- the README, which the maintainer updates, and CI.

## Public API

Changed, in `ingestio/callback/io.py`, exported as today by `ingestio` and
`ingestio.callback.all` (decision 6):

```python
class ReadSource(Callback):
    """Reads from the source that the job's configuration names, into the shared state.

    Args:
        config: Path of the job's JSON configuration file. Its `source` section names the
            system and holds the reader's parameters.
        into: Key of `Process.state` that receives the payload.
        connection: Where a PostgreSQL source lives, with its credentials. Required by the
            `postgres` system.
        client: SDK client of the source, for the systems that take one.
        watermark_value: Last watermark already ingested, for the systems that take one.
        spark: Session to read with, for the Spark systems. The active session when omitted.
    """

    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        into: str = "records",
        connection: PostgresConnection | None = None,
        client: object | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...


class WriteTarget(Callback):
    """Writes a value of the shared state to the target that the job's configuration names.

    Args:
        config: Path of the job's JSON configuration file. Its `target` section names the
            system and holds the writer's parameters.
        key: Key of `Process.state` holding the value to write.
        replace: Make the target hold exactly the value written. Appends when `False`.
        connection: Where a PostgreSQL target lives, with its credentials. Required by the
            `postgres` system.
        client: SDK client of the target, for the systems that take one.
    """

    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        key: str = "records",
        replace: bool = False,
        connection: PostgresConnection | None = None,
        client: object | None = None,
    ) -> None: ...
```

- `config` is positional; every other argument is keyword-only. `os` is the standard module;
  `Decimal` is `decimal.Decimal`; `date` is `datetime.date`, and `datetime.datetime`, its
  subclass, is accepted; `SparkSession` is `pyspark.sql.SparkSession`, imported for annotations
  only; `PostgresConnection` is `ingestio.connector.all.PostgresConnection`.
- `client` is typed `object` because its type depends on the system: see
  [Callback arguments](#callback-arguments).
- `run()` of each callback keeps its role: `ReadSource` stores the payload in `state[into]`,
  `WriteTarget` writes `state[key]`.

Removed:

- `ReadSource(reader, *, into="records")` and `WriteTarget(writer, *, key="records")`: the
  callbacks no longer accept a connector (decision 1). Passing one raises `TypeError` (see
  [The configuration file](#the-configuration-file)).

Unchanged (decisions 8 and 9):

- every connector class, `PostgresConnection`, `Reader` and `Writer`, with their import paths:
  `ingestio.connector.all` exports all of them, and `ingestio` exports `Reader` and `Writer`.
  The connector classes stay public so that a job's own callback can still use a shipped
  connector for a strategy the library's callbacks do not offer; `Reader` and `Writer` stay the
  protocols that every shipped connector satisfies, exported for the annotations of such
  callbacks;
- `Callback`, `Process`, `Step`, the cancel exceptions, `log_to_console`;
- the attributes that the callbacks set today (`reader`, `writer`, `into`, `key`) are not
  documented, so they are not public API. The connector a callback builds is not exposed.

No new module is public. The table of systems is private, in
`ingestio/callback/_configuration.py` (see [Where it lives](#where-it-lives)).

### Usage sketch

A sketch of a job, not a runnable example. `config.json`:

```json
{
  "source": {"system": "postgres", "table_name": "public.orders", "watermark_column": "updated_at"},
  "target": {"system": "unity_catalog", "table_name": "main.bronze.orders"},
  "primary_key": "id"
}
```

`main.py`:

```python
CONFIG = Path(__file__).with_name("config.json")

# Credentials come from the environment; on Databricks, from dbutils.secrets.get.
shop = PostgresConnection(
    "pg.internal", "shop", os.environ["SHOP_USER"], os.environ["SHOP_PASSWORD"]
)

Process([
    Step("extract", [ReadSource(CONFIG, connection=shop, watermark_value=last_watermark())]),
    Step("load", [SkipEmptyBatch(), WriteTarget(CONFIG)]),
]).run()
```

- `last_watermark` and `SkipEmptyBatch` are the job's, as in the README today.
- `primary_key` is a key of the job: `ReadSource` and `WriteTarget` ignore every top-level key
  other than their own section (decision 11).
- Moving the target to BigQuery is an edit of the `target` section, plus `ingestio[bigquery]` in
  the job's install, because the BigQuery writer needs that extra.

## Behaviour

### The configuration file

- **The argument.** `config` is a `str` or an `os.PathLike[str]`, such as a `pathlib.Path`.
  Anything else raises `TypeError(f"config must be a str or os.PathLike, not {name}")`, where
  `name` is `type(config).__name__`, when the callback is built. A connector passed as in the
  old form, `ReadSource(PostgresReader(...))`, gets this error, naming the connector's class.
- **Reading.** The callback reads the file once, when it is built (decision 10), as
  `Path(config).read_text(encoding="utf-8")`. A relative path is resolved against the working
  directory at that moment, as `open` does. Errors of reading (`FileNotFoundError`,
  `IsADirectoryError`, `PermissionError`, any other `OSError`, `UnicodeDecodeError`) propagate
  unchanged. Changing or deleting the file after the callback is built changes nothing: the
  process runs with what was read.
- **Parsing.** The text is parsed as JSON, RFC 8259: `NaN`, `Infinity` and `-Infinity`, which
  Python's `json` accepts by default, are rejected. When a key appears twice in an object, the
  last value wins, as in Python's `json`.
- **Shape.** The top level is a JSON object. `ReadSource` reads its `source` member and
  `WriteTarget` its `target` member, each a JSON object, the *section*. Every other top-level
  member is ignored by both, and each callback ignores the other's section, whatever it holds
  (decision 11). A file with only a `source` section is enough for a `ReadSource`.
- **Each callback reads the file for itself.** A process with a `ReadSource` and a
  `WriteTarget` built from the same path reads the file twice, once per callback.

```json
{
  "source": {"system": "<source system>", "<key>": "<value>", "...": "..."},
  "target": {"system": "<target system>", "<key>": "<value>", "...": "..."}
}
```

### Systems

A section names its system with the key `system` (decision 7), whose value is one of the names
below, exactly as written: names are case-sensitive. The name is the connector's class name,
in snake case, without `Reader` or `Writer`.

| `system` | `source` builds | `target` builds | Extra the connector may need |
|---|---|---|---|
| `json_api` | `JsonApiReader` | not a target | none |
| `json_lines` | not a source | `JsonLinesWriter` | none |
| `disk` | `DiskReader` | `DiskWriter` | none |
| `postgres` | `PostgresReader` | `PostgresWriter` | `postgres`, for the writer |
| `s3` | `S3Reader` | `S3Writer` | `s3`, for a reader without `client` |
| `gcs` | `GcsReader` | `GcsWriter` | `gcs`, for a reader without `client` |
| `volume` | `VolumeReader` | `VolumeWriter` | `databricks`, for a reader without `client` |
| `unity_catalog` | `UnityCatalogReader` | `UnityCatalogWriter` | none |
| `bigquery` | `BigQueryReader` | `BigQueryWriter` | `bigquery`, for the writer, and for a reader with `watermark_column` |

The table is closed (decision 4). It lives in the library, nothing outside the library can add
to it or replace an entry, and it is not exported. A new system is a change to the library,
through a spec of its own, which adds a row here and in the documentation.

### Keys of each system

Every key of a section other than `system` is the constructor argument of the same name of the
connector that the system builds, as
[ADR 0013](../docs/decisions/0013-connector-configuration-convention.md) names them. The value
is passed as it is: the callback renames nothing and converts nothing. A section holds only
the keys of its system:

| `system` | Section | Required keys | Optional keys |
|---|---|---|---|
| `json_api` | source | `url` | `timeout`, `headers`, `encoding` |
| `json_lines` | target | `path` | `encoding` |
| `disk` | source | `path`, `format` | `watermark_pattern`, `schema`, `options` |
| `disk` | target | `path`, `format` | `options` |
| `postgres` | source | `table_name` | `watermark_column`, `options` |
| `postgres` | target | `table_name` | `options` |
| `s3` | source | `path`, `format` | `watermark_pattern`, `schema`, `options` |
| `s3` | target | `path`, `format` | `options` |
| `gcs` | source | `path`, `format` | `watermark_pattern`, `schema`, `options` |
| `gcs` | target | `path`, `format` | `options` |
| `volume` | source | `volume`, `path`, `format` | `watermark_pattern`, `schema`, `options` |
| `volume` | target | `volume`, `path`, `format` | `options` |
| `unity_catalog` | source | `table_name` | `watermark_column`, `options` |
| `unity_catalog` | target | `table_name` | `options` |
| `bigquery` | source | `table_name` | `watermark_column`, `billing_project`, `options` |
| `bigquery` | target | `table_name` | `billing_project`, `options` |

- The required keys are the connector's positional arguments other than `connection`, in their
  order. An optional key that is left out leaves the argument at its default.
- Not configuration keys: the callback arguments, `connection`, `client`, `watermark_value` and
  `spark` (decision 3); `append` of `JsonLinesWriter`, which `WriteTarget` sets (decision 13); any
  credential, such as `user`, `password` or `host`. Like any other key a system does not take,
  they are rejected.
- `schema` is a DDL string, such as `"id INT, payload STRING"`. A `StructType` cannot be given
  (decision 14).
- Each section's keys name its own system's location, so the `target` section's `table_name`,
  `path` or `volume` is the target's. The ambiguity that ADR 0013 records does not arise in a
  configuration.
- `headers` of `json_api` is not a place for credentials: `JsonApiReader` does not support
  endpoints that require authentication.

**JSON types.** The callback checks the type of every value before it builds the connector.
`null` is never a value: a key that the job does not set is left out.

| Keys | JSON type | Message when the type is wrong |
|---|---|---|
| `system`, `url`, `encoding`, `path`, `format`, `watermark_pattern`, `schema`, `table_name`, `watermark_column`, `volume`, `billing_project` | string | `<section>.<key> must be a string` |
| `timeout` | number (`true` and `false` are not numbers) | `<section>.<key> must be a number` |
| `headers` | object whose values are strings | `<section>.<key> must be an object of strings` |
| `options` | object whose values are strings, numbers or booleans | `<section>.<key> must be an object of strings, numbers and booleans` |

JSON numbers reach the connector as Python's `json` parses them: `int` or `float`.

### Callback arguments

Values that do not belong in the JSON reach the callback as its arguments (decision 3): the job
builds them, from its own secrets, its environment or its control tables, and the callback
hands each one, the very object, to the connector's constructor argument of the same name. The
library still reads no secret.

| `system` | Section | `connection` | `client` | `watermark_value` | `spark` |
|---|---|---|---|---|---|
| `json_api` | source | no | no | no | no |
| `disk` | source | no | no | optional | optional |
| `postgres` | source | **required** | no | optional | optional |
| `s3` | source | no | optional: a boto3 S3 client | optional | optional |
| `gcs` | source | no | optional: a `google.cloud.storage.Client` | optional | optional |
| `volume` | source | no | optional: a `databricks.sdk.WorkspaceClient` | optional | optional |
| `unity_catalog` | source | no | no | optional | optional |
| `bigquery` | source | no | optional: a `google.cloud.bigquery.Client` | optional | optional |
| `json_lines` | target | no | no | – | – |
| `disk`, `s3`, `gcs`, `volume`, `unity_catalog` | target | no | no | – | – |
| `postgres` | target | **required** | no | – | – |
| `bigquery` | target | no | optional: a `google.cloud.bigquery.Client` | – | – |

- An argument that is `None` is not given, for every system. A job that passes
  `watermark_value=None` on its first run passes nothing.
- An argument given, not `None`, to a system that does not take it ("no" above) raises
  (decision 12). `WriteTarget` has no `watermark_value` and no `spark`.
- A system that falls back on a default client from the environment needs nothing: without
  `client`, the connector creates its default client as its spec says.
- The connector validates the values: a naive `datetime` as `watermark_value` raises the
  connector's own `ValueError`, as when the connector is built directly.

### Building the callback

Building a `ReadSource` or a `WriteTarget` does all of the following, in this order, and the
first problem found raises (decision 10):

1. checks `config` and reads the file;
2. parses the JSON and checks that the top level is an object;
3. checks that the section is present and is an object;
4. checks `system`: present, a string, one of the names valid for the section;
5. checks the keys, in the order of the file: each one is a key of the system;
6. checks the required keys, in the order of the table: each one is present;
7. checks the type of each value, in the order of the file;
8. checks the callback arguments: none that the system does not take is given, in the order
   `connection`, `client`, `watermark_value`, `spark`; then the required one, `connection`, is
   given;
9. builds the connector, with the section's keys and the callback arguments the system takes.

Building the connector is the connector's own building: it validates its arguments, may check
that its extra's package imports, and does nothing else. So building the callback touches no
Spark session, no client and no network, whatever the system. The job's process can therefore
not start with a configuration that its read or write callback cannot use: a mistake in the
`target` section raises when the `WriteTarget` is built, before the `ReadSource` has read
anything.

### Running

- `ReadSource.run()` calls `read()` on the connector it built and stores the result in
  `state[into]`.
- `WriteTarget.run()` writes `state[key]` with the connector it built (see
  [Append and replace](#append-and-replace)).
- Nothing is caught: every error of `read()` and `write()` propagates out of `Process.run()`
  unchanged, the same object, with no note, and the run's log shows the failed moment, as for any
  callback. A missing `state[key]` raises `KeyError`, as today.
- The callbacks implement only `run`, and write no log lines of their own.

### Append and replace

`WriteTarget` chooses the write strategy, with its `replace` argument (decision 13). The
configuration has no mode, and no section key chooses one.

- `replace=False`, the default, appends: `write(data)` for every system but `json_lines`, and a
  `JsonLinesWriter` built with `append=True`, whose `write(data)` adds lines to the file.
- `replace=True` replaces: `write(data, replace=True)` for every system but `json_lines`, and a
  `JsonLinesWriter` built with `append=False`, whose `write(data)` replaces the file.
- So `WriteTarget` appends to every target by default. Today it replaces a `JsonLinesWriter`'s
  file, the divergence recorded in the connectors page's "Known divergence"; that divergence
  ends for `WriteTarget`. `JsonLinesWriter(append=...)` itself does not change: the append-only
  spec decides its fate.
- What `write(data, replace=True)` does is the connector's, as its spec says (`TRUNCATE`, an
  overwrite, a `MERGE`).

### Errors

`<path>` is `os.fspath(config)`, and `<section>` is `source` for `ReadSource` and `target` for
`WriteTarget`. Every error of the callback's own checks is a `ValueError` whose message starts
with `<path>: `.

"Building" is the call that builds the callback, `ReadSource(...)` or `WriteTarget(...)`, which
the job makes while it builds its process: before `Process(...)` exists, and so before
`run()`. "Running" is the callback's `run` moment, inside `Process.run()` (decision 10).

| Case | Raised | Error |
|---|---|---|
| `config` is not a `str` or an `os.PathLike` | building | `TypeError: config must be a str or os.PathLike, not <type name>` |
| the file is missing, a directory, unreadable, not UTF-8 | building | the `OSError` or `UnicodeDecodeError` of reading, unchanged |
| the text is not JSON | building | `<path>: invalid JSON: <str of the JSONDecodeError>`, raised from it |
| `NaN`, `Infinity` or `-Infinity` in the text | building | `<path>: invalid JSON: <constant> is not allowed` |
| the top level is not an object | building | `<path>: the configuration must be a JSON object` |
| the section is missing | building | `<path>: missing section '<section>'` |
| the section is not an object | building | `<path>: section '<section>' must be a JSON object` |
| `system` is missing | building | `<path>: <section>.system is required` |
| `system` is not a string | building | `<path>: <section>.system must be a string` |
| `system` is not a name of the section, including a name in another case, a reader-only system as a target and a writer-only system as a source | building | `<path>: <section>.system '<name>' is not one of: <names>` |
| a key the system does not take, such as `primary_key`, `password`, `watermark_value` or `append` | building | `<path>: <section>.<key> is not a key of system '<system>'` |
| a required key is missing | building | `<path>: <section>.<key> is required for system '<system>'` |
| a value has the wrong JSON type, or is `null` | building | `<path>: <section>.<key> must be ...`, as in the JSON types of [Keys of each system](#keys-of-each-system) |
| a callback argument that the system does not take is given | building | `<path>: <section> system '<system>' takes no argument <name>` |
| `connection` is missing for `postgres` | building | `<path>: <section> system 'postgres' needs the argument connection` |
| the connector rejects a value, such as `table_name: "orders"` for `unity_catalog` | building | the connector's `ValueError`, the same object, with one note (decision 16) |
| the system's extra is not installed | building | the connector's `ImportError`, the same object, with one note (decision 16) |
| `read()` or `write()` fails | running | the error, unchanged, with no note |
| `state[key]` is missing | running | `KeyError`, as today |

- `<names>` are the names valid for the section, sorted and separated by `, `: for a source
  `bigquery, disk, gcs, json_api, postgres, s3, unity_catalog, volume`, for a target
  `bigquery, disk, gcs, json_lines, postgres, s3, unity_catalog, volume`.
- `<key>`, and `<name>` of `system`, are as written in the file. In the argument messages,
  `<name>` is the argument's name. `<type name>` is `type(config).__name__`.
- The note (decision 16) is `while building the <section> of <path>, system '<system>'`, added with
  `add_note`. The error is not wrapped: its type, message and cause stay the connector's.
- Errors that `Process` raises, such as a duplicate step name, are unchanged. A callback that
  fails to build never reaches `Process`, so its error writes no line of the run's log.

### Extras

The extras stay as they are (decision 5). A configuration that names a system whose extra is
not installed raises, when the callback is built, the `ImportError` that the connector raises
when it is built, which names the extra: `PostgresWriter needs psycopg: pip install
'ingestio[postgres]'`. It follows the connector's rule of "when the connector needs the
package": an `s3`, `gcs` or `volume` source given a `client` needs no extra, and a `bigquery`
source without `watermark_column` needs none either. So changing a job's system is an edit of
its JSON and, when the new system needs an extra, of its install.

### Where it lives

- `src/ingestio/callback/io.py`: `ReadSource` and `WriteTarget`, with the new signatures.
- `src/ingestio/callback/_configuration.py`, new and private: reading and checking the file and
  one section, the table of systems, and building the connector. The append-only and SCD2
  callbacks can share it later.
- The connector modules, `connector/all.py`, `basics.py`, `__init__.py` and `pyproject.toml` do
  not change (decisions 8 and 9).
- Importing `ingestio` imports the connector modules, which import no third-party package at
  module level, so `ingestio` still imports without pyspark and without any extra.

### Effects on the draft specs

The drafts [`append_only_callback`](append_only_callback.md) and
[`scd2_callback`](scd2_callback.md) are write strategies that take a target. They are not
edited here; when each returns to planning, it needs these changes:

- **How they get their writer.** They take the path of the configuration and build their writer
  from its `target` section, through the same private routine and the same closed table, with
  the callback arguments `connection` and `client`. Neither accepts a connector: the starting
  proposals `AppendOnly(target, *, key="records")` and `SCD2(source, target, *, keys, ...)`
  change accordingly.
- **No connector of the user's.** Decision 4 removes the case of a connector written in user
  code: append-only's AT-005 goes, and so does the idea of a new capability protocol that user
  connectors implement (its question 1, first option; SCD2's question 1, second option). A new
  capability, such as a merge, would be added to shipped connectors only.
- **Append-only.** Every shipped writer except `JsonLinesWriter` appends with `write(data)`, and
  this spec builds `JsonLinesWriter` with `append=True` for an append, which answers its
  question 1 for the shipped connectors. Its question 2, the fate of
  `JsonLinesWriter(append=...)`, stays open. It must also say how it differs from
  `WriteTarget(config)`, which appends by default under decision 13, or whether one replaces the
  other. Its AT-004, a generator consumed lazily, holds only for `json_lines`: the Spark writers
  take a DataFrame.
- **SCD2.** It reads the current versions from the target, so it needs the target system's
  reader as well as its writer. The `target` section is built for a writer: a reader of the
  same system may need keys the writer does not take, such as `watermark_column`. Its spec
  decides how it builds that reader, and which target systems it supports.
- **Their own parameters.** Under decision 11, a section holds only its system's keys, so the
  parameters of these callbacks (SCD2's `keys`, `tracked` and version columns, for instance)
  cannot go in the `target` section. They are constructor arguments of the callback, or members
  of the file outside `source` and `target`; each spec decides.
- **The configuration has no mode (decision 13).** Neither callback reads a mode from the
  section.

### What becomes outdated

The documentation stage updates `docs/` and `.claude/kb/`; the README is the maintainer's.

- `README.md`: "The idea" (a connector "injected into the callback that needs it"), "Design
  philosophy" ("no registries"), "Example: a public API into a volume" and "Writing a callback"
  (`ReadSource(JsonApiReader(...))`, `WriteTarget(JsonLinesWriter(...))`), the whole of
  "Structuring a job" (the flat `config.json`, "The library never reads it", the connectors
  built in `main.py`, the bullets on `primary_key` and the writer's `table_name`), and "Roadmap"
  (recipes "never a required one").
- `AGENTS.md`, "Philosophy in brief": "no registries".
- `docs/philosophy.md`, ADR 0003 and ADR 0013: see [ADRs](#adrs). `docs/index.md`: the new spec
  and ADR 0015, and the description of the concept pages.
- `docs/concepts/callbacks.md`: the table of the shipped callbacks.
- `docs/concepts/connectors.md`: "Using a connector" (injection, and its runnable `StaticReader`
  example, which will fail), "How a connector receives an ingestion's parameters" ("which the
  library does not read or parse"), every "`ReadSource` and `WriteTarget` use them unchanged",
  every example that injects a connector, "Known divergence", and the purpose given for
  exporting `Reader` and `Writer`. The system name and the configuration keys of each connector
  belong in its section or in one table; a page of its own, such as
  `docs/concepts/configuration.md`, would keep the tables of this spec in one place. The
  doc-keeper decides.
- `.claude/kb/architecture/quick-reference.md`: concepts, modules, the ADR 0013 lines, and the
  known gap on `WriteTarget` and `JsonLinesWriter`.
- `docs/backlog.md`, idea 11: its entry links to this spec, and its questions are answered
  here.
- `tests/test_io.py` and the composition criteria of seven validated connector specs, which use
  the removed form, and which this spec supersedes (decision 17): disk AT-027, s3 AT-032, gcs
  AT-033, volume AT-032, postgres AT-035,
  unity_catalog AT-025 and bigquery AT-037.

### Known limitations

- **One source and one target per file (decision 15).** A job with two sources uses two files.
- **The watermark is computed before the process is built.** `watermark_value` is an argument
  of the callback, built before the run, as the connector's is today. A watermark that a step of
  the same process would compute cannot reach the reader.
- **A `StructType` schema cannot be given (decision 14).** A DDL string covers every Spark type; column
  metadata other than comments is lost.
- **A key that appears twice** in a section keeps its last value, silently, as JSON parsers do.
- **The file is read when the callback is built.** A long-lived process does not see later
  changes to the file.

### Notes for testing

- Tests write each configuration with `json.dumps` to a file under `tmp_path` and pass its path.
  A test that needs invalid JSON writes the text itself.
- A *twin* is a connector built directly from `ingestio.connector.all`, with the arguments that
  the criterion names, over a second set of fakes in the same initial state. The criteria that
  map a system compare the events that the callback's connector records with those of its twin,
  in order. This observes the mapping through the public API only, without reading the
  callback's attributes. Two writers generate values of their own, which differ between the
  callback's connector and its twin: the staging table names of `PostgresWriter` and
  `BigQueryWriter`, `ingestio_staging_` followed by 32 hexadecimal digits, and the expiry time of
  `BigQueryWriter`'s staging table. The comparison treats each as equal when it matches that
  pattern, or, for the expiry, when both are a day after their write, within the test's run.
- The fakes are those of the connector specs, rebuilt in this spec's test file: a fake Spark
  session (`read`, `format`, `option`, `options`, `schema`, `load`, `table`), fake DataFrames
  (`columns`, `write` and its chain, `where`, `selectExpr`, `first`), fake clients for S3
  (`get_paginator`), GCS (`list_blobs`), Volumes (`files.list_directory_contents`) and BigQuery
  (`query`, `get_table`, `create_table`, `delete_table`, `close`), and `psycopg.connect` replaced
  with `monkeypatch.setattr`. The `disk` criteria use a real directory under `tmp_path`.
- `json_api` uses the `api_url` fixture, a local HTTP server; `json_lines` writes under
  `tmp_path`.
- A missing package is simulated in a subprocess, by mapping its top-level name (`pyspark`,
  `psycopg`, `boto3`, `google`, `databricks`) to `None` in `sys.modules`.
- `pyspark.sql.SparkSession.active` can be replaced with `monkeypatch.setattr`.

## Acceptance criteria

The criteria follow the maintainer's decisions. A criterion that rests on one of decisions 6 to
17 names it. Unless a criterion says otherwise, `path` is a configuration file under
`tmp_path`, `<path>` is `os.fspath(path)`, and every error is raised by the call that builds the
callback, `ReadSource(path, ...)` or `WriteTarget(path, ...)`, before any `Process` is built
(decision 10).

### Import surface

- **AT-001**: Given a fresh interpreter, when `from ingestio import ReadSource, WriteTarget` and
  `from ingestio.callback.all import ReadSource, WriteTarget` run, then both succeed and give the
  same classes. `ReadSource` and `WriteTarget` are in `ingestio.__all__` and in
  `ingestio.callback.all.__all__`. Every connector class, `PostgresConnection`, `Reader` and
  `Writer` are still in `ingestio.connector.all.__all__`, `Reader` and `Writer` still in
  `ingestio.__all__`, and `PostgresConnection` is not in `ingestio.__all__`. See decisions 6, 8
  and 9.
- **AT-002**: Given an interpreter where `pyspark`, `psycopg`, `boto3`, `google` and `databricks`
  are mapped to `None` in `sys.modules`, when `ingestio` is imported and a `ReadSource` is built
  for a `json_api` source and a `WriteTarget` for a `json_lines` target, then each succeeds.

### The configuration file

- **AT-003**: Given `JsonApiReader("https://api.example.com/v1/orders")` as `config`, then
  `ReadSource` raises `TypeError("config must be a str or os.PathLike, not JsonApiReader")`.
  Given a `JsonLinesWriter`, `WriteTarget` raises the same message naming `JsonLinesWriter`.
  Given `b"job.json"` or a `dict`, both raise it, naming `bytes` or `dict`. A `str` and a
  `pathlib.Path` naming a valid file are accepted.
- **AT-004**: Given a path where no file exists, when `ReadSource(path)` or `WriteTarget(path)`
  is built, then it raises `FileNotFoundError`. Given the path of a directory, it raises
  `IsADirectoryError` (or, on Windows, the `PermissionError` of reading).
- **AT-005**: Given a file holding `{"source": `, when `ReadSource(path)` is built, then it
  raises `ValueError` whose message starts with `<path>: invalid JSON: ` and whose `__cause__`
  is a `json.JSONDecodeError`. Given a valid `json_api` source with `"timeout": NaN`, then the
  message is `<path>: invalid JSON: NaN is not allowed`, and the same holds for `Infinity` and
  `-Infinity`.
- **AT-006**: Given files holding `[]`, `"text"`, `1` and `null`, when either callback is built,
  then it raises `ValueError("<path>: the configuration must be a JSON object")`.
- **AT-007**: Given `{}`, when `ReadSource(path)` is built, then it raises
  `ValueError("<path>: missing section 'source'")`, and `WriteTarget(path)` raises
  `ValueError("<path>: missing section 'target'")`. Given `"source": []` or
  `"source": "postgres"`, `ReadSource` raises
  `ValueError("<path>: section 'source' must be a JSON object")`, and the same holds for
  `target`.
- **AT-008**: Given a file with a valid `source` section, `"target": 5`, `"primary_key": "id"`
  and `"job": {"owner": "data"}`, when `ReadSource(path)` is built, then it succeeds. Given a
  valid `target` section and `"source": 5`, `WriteTarget(path)` succeeds. See decision 11.
- **AT-009**: Given `ReadSource(path)` and `WriteTarget(path)` built from a valid file holding a
  `json_api` source (the `api_url` fixture) and a `json_lines` target, when the file is then
  deleted and the process runs, then the records reach the `json_lines` file. Given a file
  rewritten after building with another `url`, the process reads the first one. See decision 10.
- **AT-010**: Given the working directory changed to `tmp_path` with `monkeypatch.chdir`, and a
  valid configuration at `tmp_path / "job.json"`, when `ReadSource("job.json")` is built and the
  working directory is then changed again, then the process reads the source of that file.

### Systems and keys

- **AT-011**: Given a `source` section without `system`, then `ReadSource` raises
  `ValueError("<path>: source.system is required")`. Given `system` as `5`, `null` or
  `["postgres"]`, it raises `ValueError("<path>: source.system must be a string")`. The same
  holds for `target` with `WriteTarget`.
- **AT-012**: Given the `system` on the left, then building raises
  `ValueError("<path>: <section>.system '<name>' is not one of: <names>")`, with `<names>` the
  sorted list of the section given under [Errors](#errors):

  | Section | `system` |
  |---|---|
  | source | `mysql`, `Postgres`, `json_lines`, `""` |
  | target | `mysql`, `BigQuery`, `json_api`, `""` |

  See decision 7.
- **AT-013**: Given a section of the system on the left that holds one more key, then building
  raises `ValueError("<path>: <section>.<key> is not a key of system '<system>'")`:

  | Section | `system` | Extra key |
  |---|---|---|
  | source | `postgres` | `primary_key`, `password`, `host`, `watermark_value`, `connection`, `spark`, `client` |
  | source | `disk` | `watermark_column`, `client` |
  | source | `json_api` | `watermark_value`, `spark` |
  | source | `unity_catalog` | `billing_project` |
  | target | `json_lines` | `append`, `replace` |
  | target | `unity_catalog` | `watermark_column`, `mode`, `replace` |
  | target | `s3` | `client`, `watermark_pattern` |

  Given two such keys, the message names the first in the order of the file. Given such a key and
  a missing required key, the message names the extra key. See decisions 11 and 13.
- **AT-014**: Given a section that lacks one required key of its system, then building raises
  `ValueError("<path>: <section>.<key> is required for system '<system>'")`, for every row and
  every required key of [Keys of each system](#keys-of-each-system). Given a section that lacks
  all of them, the message names the first in the table's order.
- **AT-015**: Given the value on the left, then building raises `ValueError` with the message on
  the right, prefixed with `<path>: `:

  | Section, `system` | Value | Message |
  |---|---|---|
  | source, `postgres` | `"table_name": 5` | `source.table_name must be a string` |
  | source, `postgres` | `"watermark_column": null` | `source.watermark_column must be a string` |
  | source, `json_api` | `"timeout": "10"` | `source.timeout must be a number` |
  | source, `json_api` | `"timeout": true` | `source.timeout must be a number` |
  | source, `json_api` | `"headers": {"X-Trace": 1}` | `source.headers must be an object of strings` |
  | source, `json_api` | `"headers": ["X-Trace"]` | `source.headers must be an object of strings` |
  | source, `disk` | `"schema": {"type": "struct", "fields": []}` | `source.schema must be a string` |
  | source, `disk` | `"options": {"header": null}` | `source.options must be an object of strings, numbers and booleans` |
  | source, `disk` | `"options": {"header": [true]}` | `source.options must be an object of strings, numbers and booleans` |
  | target, `unity_catalog` | `"options": "mergeSchema"` | `target.options must be an object of strings, numbers and booleans` |

  `"timeout": 10`, `"timeout": 10.5`, `"headers": {"X-Trace": "1"}` and
  `"options": {"fetchsize": 5000, "pushDownPredicate": true, "sessionInitStatement": "SET x"}`
  are accepted. See decision 14.

### Callback arguments

- **AT-016**: Given a `postgres` source and `ReadSource(path)` without `connection`, then it raises
  `ValueError("<path>: source system 'postgres' needs the argument connection")`. Given a
  `postgres` target and `WriteTarget(path)`, it raises the same with `target`.
- **AT-017**: Given the argument on the left, not `None`, then building raises
  `ValueError("<path>: <section> system '<system>' takes no argument <name>")`:

  | Callback | `system` | Argument |
  |---|---|---|
  | `ReadSource` | `json_api` | `connection`, `client`, `watermark_value`, `spark` |
  | `ReadSource` | `disk`, `unity_catalog` | `connection`, `client` |
  | `ReadSource` | `postgres` | `client` |
  | `ReadSource` | `s3`, `gcs`, `volume`, `bigquery` | `connection` |
  | `WriteTarget` | `json_lines`, `disk`, `s3`, `gcs`, `volume`, `unity_catalog` | `connection`, `client` |
  | `WriteTarget` | `postgres` | `client` |
  | `WriteTarget` | `bigquery` | `connection` |

  Given every one of these arguments as `None`, building succeeds (with `connection` given for
  `postgres`). See decision 12.

### Building the connector

- **AT-018**: Given a source section on the left, when `ReadSource(path)` is built, then it raises
  the `ValueError` that building the connector raises, with the connector's message, and with
  exactly one note, `while building the source of <path>, system '<system>'`:

  | Section | Message |
  |---|---|
  | `{"system": "unity_catalog", "table_name": "orders"}` | `table_name must be 'catalog.schema.table'` |
  | `{"system": "disk", "path": "landing/orders", "format": "csv"}` | `path must be an absolute local path` |
  | `{"system": "s3", "path": "s3://acme/orders", "format": "delta"}`, `client=object()` | `format 'delta' cannot be read file by file` |
  | `{"system": "postgres", "table_name": "public.orders", "watermark_column": "updated_at"}`, `connection=conn`, `watermark_value=datetime(2026, 10, 1)` | `watermark_value must be timezone-aware` |

  Given the target section `{"system": "bigquery", "table_name": "acme.lake.orders", "options":
  {"table": "x"}}`, `WriteTarget(path)` raises `option 'table' is set by the connector`, with the
  note `while building the target of <path>, system 'bigquery'`. See decision 16.
- **AT-019**: Given an interpreter where the package on the left is mapped to `None` in
  `sys.modules`, when the callback is built for the configuration in the middle, then it raises
  the connector's `ImportError` on the right, with exactly one note
  `while building the <section> of <path>, system '<system>'`:

  | Missing | Configuration and arguments | `ImportError` message |
  |---|---|---|
  | `boto3` | `s3` source, no `client` | `S3Reader needs boto3: pip install 'ingestio[s3]'` |
  | `google` | `gcs` source, no `client` | `GcsReader needs google-cloud-storage: pip install 'ingestio[gcs]'` |
  | `databricks` | `volume` source, no `client` | `VolumeReader needs databricks-sdk: pip install 'ingestio[databricks]'` |
  | `psycopg` | `postgres` target, `connection=conn` | `PostgresWriter needs psycopg: pip install 'ingestio[postgres]'` |
  | `google` | `bigquery` target | `BigQueryWriter needs google-cloud-bigquery: pip install 'ingestio[bigquery]'` |
  | `google` | `bigquery` source with `watermark_column` | `BigQueryReader needs google-cloud-bigquery: pip install 'ingestio[bigquery]'` |

  In the same interpreters, an `s3`, `gcs` or `volume` source given `client=object()`, a
  `postgres` source, a `bigquery` source without `watermark_column`, and an `s3`, `gcs` or
  `volume` target build without error. See decision 16.
- **AT-020**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises, and
  fake sessions and clients that record every attribute accessed on them, when a `ReadSource` is
  built for every source row of [AT-022](#reading) and a `WriteTarget` for every target row of
  [AT-024](#writing), then each succeeds, `active` is not called, and nothing is recorded on the
  fakes.

### Reading

- **AT-021**: Given `{"source": {"system": "json_api", "url": "<api_url>", "timeout": 5,
  "headers": {"X-Trace": "1"}}}`, when `Process([Step("extract", [ReadSource(path,
  into="orders")])]).run()` runs, then `state["orders"]` equals the payload that the server
  serves.
- **AT-022**: Given the source section and the callback arguments of a row below, when
  `Process([Step("extract", [ReadSource(path, into="orders", **arguments)])]).run()` runs, then
  the fakes record the same events, in the same order, as `twin.read()`, where `twin` is the
  connector on the right built over fakes in the same initial state. `state["orders"]` is the
  object that the callback's connector returned: the one returned by the call that matches the
  call whose result `twin.read()` returned (a load, a `where`, or `createDataFrame` for an empty
  selection).

  | `system` | Section, besides `system` | Arguments | `twin` |
  |---|---|---|---|
  | `disk` | `"path": "<tmp>/landing"`, `"format": "csv"`, `"watermark_pattern": "orders_(\\d{8})\\.csv"`, `"schema": "id INT, payload STRING"`, `"options": {"header": true}` | `watermark_value="20261001"`, `spark=spark` | `DiskReader("<tmp>/landing", "csv", watermark_pattern=..., watermark_value="20261001", schema="id INT, payload STRING", options={"header": True}, spark=spark)` |
  | `postgres` | `"table_name": "public.orders"`, `"watermark_column": "updated_at"`, `"options": {"fetchsize": 5000}` | `connection=conn`, `watermark_value=datetime(2026, 10, 1, tzinfo=UTC)`, `spark=spark` | `PostgresReader("public.orders", conn, watermark_column="updated_at", watermark_value=..., options={"fetchsize": 5000}, spark=spark)` |
  | `s3` | `"path": "s3://acme-landing/orders"`, `"format": "json"`, `"options": {"multiLine": true}` | `client=s3`, `watermark_value=datetime(2026, 10, 1, tzinfo=UTC)`, `spark=spark` | `S3Reader("s3://acme-landing/orders", "json", watermark_value=..., options={"multiLine": True}, client=s3, spark=spark)` |
  | `gcs` | `"path": "gs://acme-landing/orders"`, `"format": "json"`, `"watermark_pattern": "orders_(\\d{8})\\.json"` | `client=gcs`, `watermark_value="20261001"`, `spark=spark` | `GcsReader("gs://acme-landing/orders", "json", watermark_pattern=..., watermark_value="20261001", client=gcs, spark=spark)` |
  | `volume` | `"volume": "main.landing.raw"`, `"path": "orders"`, `"format": "parquet"` | `client=workspace`, `watermark_value=datetime(2026, 10, 1, tzinfo=UTC)`, `spark=spark` | `VolumeReader("main.landing.raw", "orders", "parquet", watermark_value=..., client=workspace, spark=spark)` |
  | `unity_catalog` | `"table_name": "main.bronze.orders"`, `"watermark_column": "updated_at"`, `"options": {"versionAsOf": 3}` | `watermark_value=41`, `spark=spark` | `UnityCatalogReader("main.bronze.orders", watermark_column="updated_at", watermark_value=41, options={"versionAsOf": 3}, spark=spark)` |
  | `bigquery` | `"table_name": "acme-data.sales.orders"`, `"watermark_column": "updated_at"`, `"billing_project": "acme-billing"`, `"options": {"maxParallelism": 4}` | `client=bq`, `watermark_value=date(2026, 10, 1)`, `spark=spark` | `BigQueryReader("acme-data.sales.orders", watermark_column="updated_at", watermark_value=..., billing_project="acme-billing", options={"maxParallelism": 4}, client=bq, spark=spark)` |

- **AT-023**: Given each row of AT-022 with only the required keys of its system in the section,
  the arguments `connection=conn` for `postgres` and `client=<fake>` for `s3`, `gcs` and `volume`
  and no other argument, and `pyspark.sql.SparkSession.active` replaced with a function that
  returns the fake session, when the process runs, then the fakes record the same events as the
  twin built with the same required arguments and every optional one at its default.

### Writing

- **AT-024**: Given the target section and the callback arguments of a row below, and
  `state={"orders": df}` where `df` is a fake DataFrame with `columns == ["id", "payload"]`, when
  `Process([Step("load", [WriteTarget(path, key="orders", **arguments)])], state=...).run()`
  runs, then the fakes record the same events, in the same order, as `twin.write(df)` over fakes
  in the same initial state, with the names and times that the connector generates compared as
  described in [Notes for testing](#notes-for-testing). With `replace=True` added to the
  arguments, they record the same events as `twin.write(df, replace=True)`. See decision 13.

  | `system` | Section, besides `system` | Arguments | `twin` |
  |---|---|---|---|
  | `disk` | `"path": "<tmp>/lake/orders"`, `"format": "parquet"`, `"options": {"compression": "snappy"}` | none | `DiskWriter("<tmp>/lake/orders", "parquet", options={"compression": "snappy"})` |
  | `postgres` | `"table_name": "public.orders_copy"`, `"options": {"batchsize": 1000}` | `connection=conn` | `PostgresWriter("public.orders_copy", conn, options={"batchsize": 1000})` |
  | `s3` | `"path": "s3://acme-lake/orders"`, `"format": "delta"` | none | `S3Writer("s3://acme-lake/orders", "delta")` |
  | `gcs` | `"path": "gs://acme-lake/orders"`, `"format": "delta"` | none | `GcsWriter("gs://acme-lake/orders", "delta")` |
  | `volume` | `"volume": "main.lake.curated"`, `"path": "orders"`, `"format": "parquet"` | none | `VolumeWriter("main.lake.curated", "orders", "parquet")` |
  | `unity_catalog` | `"table_name": "main.silver.orders"`, `"options": {"mergeSchema": true}` | none | `UnityCatalogWriter("main.silver.orders", options={"mergeSchema": True})` |
  | `bigquery` | `"table_name": "acme-data.lake.orders"`, `"billing_project": "acme-billing"` | `client=bq` | `BigQueryWriter("acme-data.lake.orders", billing_project="acme-billing", client=bq)` |

- **AT-025**: Given `{"target": {"system": "json_lines", "path": "<tmp>/out/orders.jsonl"}}`, a
  file at that path holding the line `{"id": 0}`, and `state={"records": [{"id": 1}]}`, when
  `WriteTarget(path)` runs, then the file holds `{"id": 0}` and then `{"id": 1}`, one per line.
  With `replace=True`, it holds only `{"id": 1}`. Given `"encoding": "utf-16"` and a missing
  `out` directory, the directory is created and the file decodes as UTF-16. See decision 13.

### Composition

- **AT-026**: Given `ReadSource(path)` and `WriteTarget(path)` without `into` and `key`, then the
  payload goes into `state["records"]` and `WriteTarget` writes `state["records"]`. With
  `into="raw"` and `key="raw"`, `state["raw"]` is used instead.
- **AT-027**: Given one file with `{"source": {"system": "json_api", "url": "<api_url>"},
  "target": {"system": "json_lines", "path": "<tmp>/bronze/orders.jsonl"}}`, when
  `Process([Step("extract", [ReadSource(path)]), Step("load", [WriteTarget(path)])]).run()`
  runs, then the file holds one JSON line per record of the payload, in order, and
  `state["records"]` equals those records.
- **AT-028**: Given `{"source": {"system": "unity_catalog", "table_name": "main.bronze.orders"},
  "target": {"system": "unity_catalog", "table_name": "main.silver.orders"}}` and a fake session,
  when `ReadSource(path, spark=spark)` and `WriteTarget(path)` run in one process, then the only
  table read is of `` `main`.`bronze`.`orders` `` and the only table save is to
  `` `main`.`silver`.`orders` ``, of the DataFrame the read returned.
- **AT-029**: Given a `json_api` source whose `url` is the `/boom` path of the `api_url` server,
  when the process runs, then `Process.run()` raises the `urllib.error.HTTPError` of the read,
  with no note added. Given a `disk` target whose fake DataFrame's save raises `err`, then
  `Process.run()` raises `err` itself, with no note added.

## Decisions

Taken by the maintainer on 2026-10-23. They are fixed and are not reopened by this spec:

1. **The configuration becomes the only way.** The explicit form, `ReadSource(PostgresReader(...))`
   and `WriteTarget(writer)`, goes away: the read and write callbacks no longer accept a
   connector instance. This is a breaking change to the public API; the package is at 0.2.0,
   alpha.
2. **The callback receives the path of the JSON configuration file and reads the file
   itself.** `main.py` does not parse the configuration.
3. **What does not belong in the JSON reaches the callback as constructor arguments of the
   callback:** credentials (for example a `PostgresConnection` the job builds from its own
   secrets, `dbutils.secrets.get` or the environment), SDK clients (`client=` of S3, GCS, Volume
   and BigQuery), and per-run values such as `watermark_value`. The library still never reads
   secrets. Connectors that fall back on a default client from the environment need nothing
   extra.
4. **Only connectors shipped by the library can be used.** There is no way to plug a connector
   the job writes itself: no mapping argument, no import path in the configuration, no
   registry. Someone who needs another system contributes a connector to the library or opens a
   feature request.
5. **Extras stay as they are** (`ingestio[postgres]`, `[s3]`, `[gcs]`, `[databricks]`,
   `[bigquery]`). An earlier idea to put every integration's package in the core dependencies was
   dropped by the maintainer.

Taken by the maintainer on 2026-10-23, answering the open questions of the first draft of this
spec. The maintainer accepted the recommended option of every question:

6. **The callbacks keep their names** (question 1). `ReadSource` and `WriteTarget` keep their
   names, their import paths, `into` and `key` with their defaults; the path of the file is
   their positional argument `config`.
7. **The key is `system`, and the names come from the classes** (question 2): `json_api`,
   `json_lines`, `disk`, `postgres`, `s3`, `gcs`, `volume`, `unity_catalog`, `bigquery`, the
   connector's class name in snake case without `Reader` or `Writer`.
8. **The connector classes, `Reader` and `Writer` stay public** (question 3).
   `ingestio.connector.all` keeps exporting every connector class, and `ingestio` keeps
   exporting `Reader` and `Writer`. A job's own callback can still build a shipped connector
   for a strategy the library's callbacks do not offer; the protocols serve the annotations of
   such callbacks and are the contract of the shipped connectors. "The configuration is the only
   way" holds for the library's callbacks.
9. **`PostgresConnection` is imported from `ingestio.connector.all`, as today** (question 4). It
   does not join `ingestio`.
10. **Configuration errors are raised when the callback is built** (question 5): the callback
    reads the file, checks its section and builds the connector in its constructor, so a mistake
    anywhere in the file, a missing extra included, stops the job before anything runs.
11. **Sections are strict, the top level is free** (question 6). A section holds only its
    system's keys, and any other key fails; other top-level members are ignored, and each
    callback ignores the other's section.
12. **A callback argument that the system does not take fails** when the callback is built
    (question 7). `None` is not an argument given.
13. **`WriteTarget` chooses append or replace with `replace=False`** (question 8). The
    configuration has no mode. `WriteTarget` appends to every target by default, `json_lines`
    included, which it builds as `JsonLinesWriter(append=not replace)`; `append` is not a key.
14. **A Spark schema is a DDL string in the `schema` key, and nothing else** (question 9). A
    `StructType` can no longer be given to a file reader through `ReadSource`.
15. **One source and one target per file** (question 10). A job with two sources uses two files.
16. **An error that the connector raises while the callback builds it gets one note** naming the
    configuration (question 11): the same object, with
    `while building the <section> of <path>, system '<system>'` added by `add_note`. Its type,
    message and cause stay the connector's.
17. **This spec supersedes the tests and criteria of the removed form** (question 13):
    `tests/test_io.py`, and the composition criteria of seven validated connector specs, disk
    AT-027, s3 AT-032, gcs AT-033, volume AT-032, postgres AT-035, unity_catalog AT-025 and
    bigquery AT-037. The test-writer removes those tests, whose behaviour AT-022, AT-024, AT-026
    and AT-027 cover. The main session records in the History of each of those specs that the
    criterion is superseded by `config_connectors`; the specs keep their status, and the planner
    does not edit them.

The reasons the maintainer gave for decisions 1, 2 and 4 (question 12) are under
[ADRs](#adrs), for ADR 0015.

Proposed by this spec:

18. **The keys are the constructor arguments, by name.** Every key of a section but `system` is
    the connector's constructor argument of the same name, passed as parsed, with no renaming
    and no conversion. This keeps ADR 0013's convention as the single source of the key names:
    the connectors' documentation of their arguments is the documentation of the keys.
19. **The values that are not keys are exactly the callback arguments** `connection`, `client`,
    `watermark_value` and `spark` on `ReadSource`, and `connection` and `client` on
    `WriteTarget`. `spark` follows decision 3: a session is not JSON. Each is passed as the very
    object; `None` means not given. `WriteTarget` takes no `spark`, because no writer does.
20. **`config` is a `str` or an `os.PathLike[str]`**, read as UTF-8, relative to the working
    directory when the callback is built. Anything else raises `TypeError` naming its type, which
    also tells a job still written in the old form what changed.
21. **JSON as RFC 8259.** `NaN`, `Infinity` and `-Infinity` are rejected; a duplicate key keeps
    its last value, as Python's `json` does.
22. **The callback checks the JSON type of every value, and `null` is never a value.** Without
    the check, `"table_name": 5` would fail inside a connector with an `AttributeError` that
    names no key. Rejecting `null` gives one rule: a key the job does not set is left out.
23. **Every error of the callback's own checks is a `ValueError` prefixed with the path,** with
    the messages and the order of [Errors](#errors) and [Building the
    callback](#building-the-callback). No new exception class, as the connectors define none.
24. **Errors of `read()` and `write()` propagate unchanged, with no note,** and the callbacks
    catch nothing and log nothing of their own, as today.
25. **The table of systems is private and closed,** in `ingestio/callback/_configuration.py`,
    with the names and rows of [Systems](#systems). It is not exported, and nothing outside the
    library can add to it. A new system is a spec of its own.
26. **The connector a callback builds is not exposed,** so that what a callback holds stays free
    to change; the twins of the tests observe the mapping through the public API.
27. **Each callback reads the file for itself,** once, when it is built. Two callbacks of one
    process read it twice; sharing a parsed file would need a cache or an object passed between
    them, which decision 2 does not ask for.

### ADRs

The decisions of the maintainer change the philosophy, which only an ADR can do. The
doc-keeper writes these in the documentation stage, for the maintainer's approval with the
spec.

- **New: ADR 0015, "Choose the reader and the writer from the job's configuration file".**
  Status accepted with this spec. Evidence: this spec, decisions 1 to 17, taken by the
  maintainer on 2026-10-23, and the reasons the maintainer gave the same day. It records:
  - **Context:** jobs read their JSON in `main.py`, built connectors and injected them (README,
    ADR 0013); the philosophy ruled out registries and asked for connectors that users extend;
    ADR 0013 left the `table_name` ambiguity unresolved.
  - **Decision:** decisions 1 to 5; the `source` and `target` sections and the `system` key
    (decision 7); the closed table as the one registry the library has; users extend callbacks,
    not connectors.
  - **Rationale:** the maintainer's request, quoted in [Motivation](#motivation), and the
    reasons the maintainer gave on 2026-10-23, for the doc-keeper to quote:
    - **The configuration as the only way (decision 1):** simplicity. The job's code should be
      as clean as possible: it declares the source and the target in its configuration and
      places callbacks.
    - **The callback reads the file (decision 2):** `main.py` does not have to parse anything.
    - **Only the connectors the library ships (decision 4):** a system that is missing comes in
      through a contribution to the library or a feature request, not through the job's code.
  - **Consequences:** the philosophy's changes below; ADR 0003 and ADR 0013 amended; the
    ambiguity of `table_name` ends in a configuration; the change breaks the public API; a
    system the library lacks needs a contributed connector; nothing closes the door to
    streaming, because the protocols and their payload do not change and a streaming
    connector would be one more shipped system.
- **The philosophy, `docs/philosophy.md`.** Three sentences change, with a link to ADR 0015:
  - Under **Connector**, after "it does not decide write strategy, format or architecture.",
    add: "The library ships every connector. A job names its source and target systems in its
    configuration, and the library's read and write callbacks build the matching connector."
  - Under **Principles, Simplicity**, "No internal frameworks, registries or magic." becomes
    "No internal frameworks, open registries or magic. The one mapping from names to classes is
    the closed table of the connectors the library ships, which its read and write callbacks
    use."
  - Under **Principles, The user's autonomy**, "Extending connectors and callbacks must be as
    simple as possible: no registries, no extra configuration." becomes "Extending callbacks
    must be as simple as possible: no registries, no extra configuration. Users do not extend
    connectors: a system the library does not integrate gets a connector contributed to the
    library."
- **ADR 0013, amended** (its Evidence line records the amendment, as it does for the earlier
  ones):
  - in the first Decision bullet, "Mapping configuration keys to arguments is the job's task, and
    a later spec covers the configuration file and recipes." becomes "The library's read and
    write callbacks read the job's configuration file and map the keys of its `source` and
    `target` sections to these arguments (ADR 0015)." The rest of the bullet stays: the
    connector itself never reads files, environment variables or secret stores;
  - the credentials bullet adds that they reach the read or write callback as its arguments,
    built by the job;
  - the Rationale keeps the maintainer's words, with a line saying that since ADR 0015 the
    callback, not `main.py`, reads the configuration, and the connector still does not;
  - the consequences on `table_name`, `path` and `volume` naming the target say that in a
    configuration each section names its own system, so the ambiguity does not arise; it
    remains only for a job's own callback that builds a writer itself (decision 8);
  - "A key that a connector does not use is not an argument" adds that in a configuration such
    a key is rejected.
- **ADR 0003, amended:** "A connector is injected into the callback that needs it" becomes "The
  library's read and write callbacks build their connector from the job's configuration (ADR
  0015); a job's own callback may build a shipped connector itself." The consequence "A
  connector written in user code needs no import from ingestio" goes, and "The two concepts are
  extended in different ways" becomes "Users extend callbacks, by subclassing. Connectors are
  extended in the library, where the protocols are the contract every shipped connector
  satisfies."
- **Unchanged:** ADR 0002 (no dependency, extras as they are; the doc-keeper may add one
  consequence: a configuration needs the extra of its system when the connector would import
  the package), ADR 0004 (the state keys and their defaults stay, decision 6), ADR 0005, ADR 0011
  (the two callbacks stay essential by default), ADR 0012 (nothing closes the door to
  streaming; the watermark is still fixed when the reader is built, now inside the callback)
  and ADR 0014.

## Open questions

None. The maintainer answered all thirteen questions of the first draft on 2026-10-23,
accepting every recommendation: decisions 6 to 17 record the answers to questions 1 to 11 and
13, and [ADRs](#adrs) records the reasons given for question 12.

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-23 | planning | draft | Spec written from the maintainer's five decisions (config as the only way, the callback reads the JSON file, runtime values as callback arguments, only shipped connectors, extras unchanged). `ReadSource(config, ...)` and `WriteTarget(config, ...)` build the connector named by the `source` or `target` section through a closed, private table of 9 systems. 29 acceptance criteria, 15 decisions, ADR 0015 plus amendments to the philosophy, ADR 0003 and ADR 0013, and 13 open questions for the maintainer. |
| 2026-10-23 | planning | draft | Revised with the maintainer's answers: every recommendation accepted. The answers are decisions 6 to 17, the maintainer's reasons for decisions 1, 2 and 4 are in the ADRs section for ADR 0015, the proposed decisions became 18 to 27, and Open questions is empty. No acceptance criterion changed. |
| 2026-10-23 | approval | approved | Approved explicitly by the maintainer. |
| 2026-10-23 | tests | tested | The test-writer wrote `tests/test_config_connectors.py`: 49 tests, 237 cases covering AT-001 to AT-029, with a shared call log and twin connectors to check how each system maps to its connector, and no network. Removed `tests/test_io.py` and the seven superseded composition tests (decision 17). Suite: 1008 passed, 79 skipped, 231 failed, all in the new file and for the missing feature; AT-001 and five AT-020 writer rows pass by design. ruff clean; mypy's 38 errors all come from today's signatures. |
| 2026-10-23 | documentation | documented | The doc-keeper added `docs/concepts/configuration.md` (file shape, systems, keys, JSON types, callback arguments, append and replace, errors, extras) and ADR 0015, amended the philosophy and ADRs 0003 and 0013, rewrote `docs/concepts/callbacks.md`, `docs/concepts/connectors.md` (every connector section gives its `system` name and keys) and the kb, and updated `docs/index.md`. Returned once by the main session: a signature block in a `python` fence could never run, now a `text` fence. Docs examples: 22 passed, 2 fail for the missing feature. ADR 0015 keeps one `TODO(maintainer)`, on the reasons for decision 5 and for a closed table. |
| 2026-10-23 | implementation | implemented | The implementer added the private, closed table of systems and `build_connector` in `src/ingestio/callback/_configuration.py`, rewrote `ReadSource` and `WriteTarget` in `src/ingestio/callback/io.py` to build their connector from the file when built (`WriteTarget` with `replace`, appending by default), and reworded the docstring of `src/ingestio/connector/core.py`. Suite 1241 passed, 79 skipped, docs examples included; ruff, ruff format and mypy clean. |
| 2026-10-23 | validation | validated | The acceptance-validator passed all seven checks: AT-001 to AT-029 covered by 237 passing cases; full suite 1241 passed, 79 opt-in integration skips, all 24 documentation examples passing; ruff, format, mypy strict and `uv lock --check` clean. Code, documentation, the closed table and the amended philosophy agree; the seven superseded connector criteria are the only ones without a test. Non-blocking notes: the empty `__all__` of `_configuration.py`, a private exception as the cause of the `NaN` error, mutable table dicts, an undefined `last_watermark()` in a skipped example; `README.md` and `AGENTS.md` outdated. |
| 2026-10-23 | return | tested | Returned by the main session, under the maintainer's carte blanche of 2026-10-23, to settle the validator's non-blocking notes: to the doc-keeper for the undefined `last_watermark()` in a skipped example and the `TODO(maintainer)` of ADR 0015, then to the implementer for `__all__`, the cause of the `NaN` error and the mutable tables. |
| 2026-10-23 | documentation | documented | The doc-keeper replaced the `TODO(maintainer)` of ADR 0015 with the maintainer's reasons for decision 5 and a pointer to decision 4 for the closed table, and gave the skipped full-job example of `docs/concepts/configuration.md` a typed `last_watermark()` stub, the job's own. Docs examples: 24 passed. |
| 2026-10-23 | implementation | implemented | The implementer settled the three notes in `src/ingestio/callback/_configuration.py`: `__all__` lists `build_connector`, the `NaN` error is raised `from None`, and the tables are read-only `MappingProxyType`s. Suite 1241 passed, 79 skipped; ruff, format and mypy clean. |
| 2026-10-23 | validation | validated | The acceptance-validator passed all seven checks again after the return: AT-001 to AT-029 in 237 passing cases, full suite 1241 passed and 79 opt-in integration skips, all 24 documentation examples passing, ruff, format, mypy strict and `uv lock --check` clean. The five notes are settled and nothing else changed. The main session's rewrite of `README.md`, whose examples were run as written, and of `AGENTS.md` agrees with the spec, the docs and the amended philosophy. |
| 2026-10-23 | superseded in part | validated | Recorded by the main session under decision 21 of [append_only_callback](append_only_callback.md): the parts of this spec that build a `WriteTarget` are superseded by `AppendOnly` and `FullLoad`. That means criteria AT-001 (its `WriteTarget` import), AT-002 to AT-004, AT-006 to AT-009, AT-011 to AT-019 (their target rows), AT-020 (its writer rows), AT-024 to AT-028 and AT-029 (its write); decision 6, for `WriteTarget`; and decision 13, except its rule that the configuration has no mode. Their `WriteTarget` cases were removed from `tests/test_config_connectors.py`. Every `ReadSource` part stands, and this spec keeps its status. |
| 2026-10-24 | superseded in part | validated | Recorded by the main session under decision 18 of [scd2_callback](scd2_callback.md), approved on 2026-10-24: decision 11 is superseded in the part that leaves the whole top level of the file to the job. `SCD2` reads a top-level `scd2` section (decisions 6 and 29 there, ADR 0017), and the top-level names of the library's sections are reserved. Sections stay strict, and `ReadSource`, `AppendOnly` and `FullLoad` still ignore every member other than their own section, `scd2` included. No test of this spec is superseded, and this spec keeps its status. |
