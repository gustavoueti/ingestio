# Configuration

A job names its source and its target in a JSON configuration file. `ReadSource` and
the write callbacks, `AppendOnly`, `FullLoad` and `SCD2`, take the path of that file, read it and
build the [connector](connectors.md) it names, so the code of the job only places callbacks.
Changing the source from disk to S3 is an edit of the JSON. The decisions are recorded in [ADR
0015](../decisions/0015-choose-connectors-from-the-job-configuration.md), [ADR
0016](../decisions/0016-make-each-write-strategy-one-callback.md) and [ADR
0017](../decisions/0017-read-a-library-callbacks-own-parameters-from-a-section-named-after-it.md).

`ReadSource`, `AppendOnly`, `FullLoad` and `SCD2` are the only way the library's callbacks reach a
connector: they do not accept a connector instance, and only the connectors the library ships can be named. A job's
own callback can still build a shipped connector itself, as [Using a
connector](connectors.md#using-a-connector) shows.

## The file

```json
{
  "source": {"system": "postgres", "table_name": "public.orders", "watermark_column": "updated_at"},
  "target": {"system": "unity_catalog", "table_name": "main.bronze.orders"},
  "primary_key": "id"
}
```

- The top level is a JSON object. `ReadSource` reads its `source` member and the write callbacks
  their `target` member, each a JSON object, the *section*. A file with only a `source` section is
  enough for a `ReadSource`, and a file with only a `target` section for `AppendOnly` or
  `FullLoad`. `SCD2` also reads the `scd2` section, [described below](#the-scd2-section).
- The names `source`, `target` and `scd2` are the library's. Every other top-level member, such as
  `primary_key` above, belongs to the job: every callback ignores it, and each ignores the sections
  of the others, whatever they hold. A library callback with parameters of its own reads them from
  one section named after its class in snake case, and no other callback reads it ([ADR
  0017](../decisions/0017-read-a-library-callbacks-own-parameters-from-a-section-named-after-it.md)).
  The names the library reserves are therefore a closed set, derived from its callbacks, and a job
  with a member of its own named `scd2` renames it before placing `SCD2`.
- A section names its system with the key `system`. Every other key of the section is the
  constructor argument of the same name of the connector that the system builds, passed as it
  is: nothing is renamed or converted. The documentation of a connector's arguments is the
  documentation of its keys.
- A section holds only the keys of its system. A key that the system does not take, a
  credential such as `password` included, is an error.
- Each section names its own system's location, so the `target` section's `table_name`, `path` or
  `volume` is the target's. The ambiguity that [ADR
  0013](../decisions/0013-connector-configuration-convention.md) records does not arise.
- One source and one target per file. A job with two sources uses two files.
- The format is JSON as RFC 8259: `NaN`, `Infinity` and `-Infinity` are rejected. When a key
  appears twice in an object, the last value wins, as in Python's `json`.

## Using it in a job

```text
ReadSource(config, *, into="records", connection=None, client=None, watermark_value=None, spark=None)
AppendOnly(config, *, key="records", connection=None, client=None)
FullLoad(config, *, key="records", connection=None, client=None)
SCD2(config, *, key="records", connection=None, client=None)
```

All four are exported by `ingestio` and by `ingestio.callback.all`.

- `config` is the path of the file, a `str` or an `os.PathLike[str]` such as a `pathlib.Path`,
  and the only positional argument. A relative path is resolved against the working directory
  when the callback is built. Anything else raises `TypeError`, a connector instance included.
- `into` is the key of `Process.state` that receives the payload, and `key` the key that
  `AppendOnly`, `FullLoad` or `SCD2` writes. Both default to `"records"`.
- The write callbacks take no `replace`, `spark` or `watermark_value`: which of them to place is
  the choice of write strategy. `SCD2` takes no column names either: they are in its `scd2` section.
- The callback reads the file once, when it is built. Changing or deleting it afterwards changes
  nothing: the process runs with what was read. Each callback reads the file for itself, so a
  process with a `ReadSource` and an `AppendOnly` built from the same path reads it twice.
  `SCD2` reads it once for both of its sections.
- Building a callback reads and checks the file and builds the connector, and nothing else: it
  touches no Spark session, no client and no network. A mistake anywhere in the file raises when
  the callback is built, before `Process` exists and before anything runs.
- Every callback is essential ([ADR
  0011](../decisions/0011-callbacks-as-stages-with-criticality.md)): nothing is caught, and
  every error of `read()` and `write()` propagates out of `Process.run()` unchanged.

### What does not belong in the file

Credentials, SDK clients and per-run values are not JSON. The job builds them, from its own
secrets, its environment or its control tables, and passes them as arguments of the callback.
The callback hands each one, the very object, to the connector's constructor argument of the same
name. The library still reads no secret.

- `connection`: a [`PostgresConnection`](connectors.md#postgresconnection), required by the
  `postgres` system.
- `client`: the SDK client of the system, for the systems that take one.
- `watermark_value`: the last watermark already ingested. `ReadSource` only.
- `spark`: the session to read with. `ReadSource` only, for the Spark systems; the active session
  when omitted.

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
- An argument that is given, and not `None`, to a system that does not take it ("no" above)
  raises.
- Without `client`, the systems that fall back on a default client create it as their connector
  does: see [the connector](connectors.md).
- The connector validates the values: a naive `datetime` as `watermark_value` raises the
  connector's own `ValueError`, as when the connector is built directly.

## Systems and keys

The name of a system is the connector's class name in snake case, without `Reader` or `Writer`.
Names are case-sensitive. The table is closed: it lives in the library, nothing outside the library
can add to it or replace an entry, and it is not exported. A new system is a change to the library.

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

The keys of a section, besides `system`:

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

- The required keys are the connector's positional arguments other than `connection`. A key that
  is left out leaves the argument at its default.
- Not keys: the callback arguments above (`connection`, `client`, `watermark_value`, `spark`),
  `append`, `replace` and `mode`, and any credential, such as `user`, `password` or `host`. Like
  any other key a system does not take, they are rejected.
- `schema` is a DDL string, such as `"id INT, payload STRING"`. A `StructType` cannot be given
  through `ReadSource`.
- `headers` of `json_api` is not a place for credentials: `JsonApiReader` does not support
  endpoints that require authentication.
- The configuration has no mode: whether a write appends, replaces or keeps a history is the choice
  of the callback the job places, `AppendOnly`, `FullLoad` or `SCD2`. The `scd2` section chooses no
  write strategy.
- `SCD2` supports only the `unity_catalog`, `postgres` and `bigquery` targets; see [Callbacks](callbacks.md#supported-target-systems).

### JSON types

The callback checks the type of every value before it builds the connector. `null` is never a
value: a key that the job does not set is left out. Numbers reach the connector as Python's `json`
parses them, `int` or `float`.

| Keys | JSON type | Message when the type is wrong |
|---|---|---|
| `system`, `url`, `encoding`, `path`, `format`, `watermark_pattern`, `schema`, `table_name`, `watermark_column`, `volume`, `billing_project` | string | `<section>.<key> must be a string` |
| `timeout` | number (`true` and `false` are not numbers) | `<section>.<key> must be a number` |
| `headers` | object whose values are strings | `<section>.<key> must be an object of strings` |
| `options` | object whose values are strings, numbers or booleans | `<section>.<key> must be an object of strings, numbers and booleans` |

## Append and replace

The job chooses the write strategy by the callback it places.

- `AppendOnly` calls `write(data)` on the writer it built: it adds the data to the target and
  leaves what the target holds as it is.
- `FullLoad` calls `write(data, replace=True)`: the target ends up holding exactly the data.
- Both do so for every system, `json_lines` included, with no branch on the writer's type. What
  `write(data)` and `write(data, replace=True)` do is the connector's, as its section says
  (`TRUNCATE`, an overwrite, a `MERGE`).
- A job that appends to one target and replaces another uses two configuration files, one per
  callback.

## The `scd2` section

A top-level member of the file, a JSON object, read by `SCD2` only. `ReadSource`, `AppendOnly` and
`FullLoad` ignore it, whatever it holds, and `SCD2` ignores `source` and every other member. A file
whose `target` is written by `AppendOnly` may hold an `scd2` section.

```json
{
  "target": {"system": "unity_catalog", "table_name": "main.gold.dim_customer"},
  "scd2": {
    "key_columns": ["customer_id"],
    "effective_time_column": "updated_at",
    "tracked_columns": ["name", "city", "segment"]
  }
}
```

| Key | Required | JSON type | Meaning |
|---|---|---|---|
| `key_columns` | yes | non-empty array of non-empty strings, no column twice | the columns whose values identify an entity, its business key |
| `effective_time_column` | yes | non-empty string | the column of the record that holds the time from which the record's state holds |
| `tracked_columns` | no | non-empty array of non-empty strings, no column twice | the columns whose change opens a new version. Every column of the batch except the key columns and the effective-time column when left out |
| `valid_from_column` | no | non-empty string | the version column that holds the start of a version. `valid_from` when left out |
| `valid_to_column` | no | non-empty string | the version column that holds the end of a version, `null` while the version is current. `valid_to` when left out |

- The section is strict, as `source` and `target` are: a key not in the table is an error, and
  `null` is never a value.
- The names are column names, compared with each other and with the columns of the batch and of the
  target ignoring case, as Spark resolves them by default. A name is one top-level column: a `.` in
  it is part of the name, not a nested field.
- No column has two roles. The key columns, the effective-time column, the tracked columns and the
  two version columns, the defaults included, name pairwise different columns.
- The section names no system and chooses no write strategy: the configuration still has no mode.
- The target must hold the batch's columns and the two version columns, and nothing else. The
  effective-time column keeps the record's own value, and the callback checks no type.
  [Callbacks](callbacks.md#scd2) describes what `SCD2` does with them.

Changing the key columns or the tracked columns is an edit of the JSON. One dimension per file.

## Errors

`<path>` is `os.fspath(config)` and `<section>` is `source` for `ReadSource` and `target` for
`AppendOnly`, `FullLoad` and `SCD2`. Every error of the callback's own checks is a `ValueError` whose message starts with
`<path>: `. The callback finds the first problem in this order, and raises it when it is built:

| Case | Error |
|---|---|
| `config` is not a `str` or an `os.PathLike` | `TypeError: config must be a str or os.PathLike, not <type name>` |
| the file is missing, a directory, unreadable, not UTF-8 | the `OSError` or `UnicodeDecodeError` of reading, unchanged |
| the text is not JSON | `<path>: invalid JSON: <message of the JSONDecodeError>`, raised from it |
| `NaN`, `Infinity` or `-Infinity` in the text | `<path>: invalid JSON: <constant> is not allowed` |
| the top level is not an object | `<path>: the configuration must be a JSON object` |
| the section is missing | `<path>: missing section '<section>'` |
| the section is not an object | `<path>: section '<section>' must be a JSON object` |
| `system` is missing | `<path>: <section>.system is required` |
| `system` is not a string | `<path>: <section>.system must be a string` |
| `system` is not a name of the section, a name in another case, a reader-only system as a target or a writer-only system as a source included | `<path>: <section>.system '<name>' is not one of: <names>` |
| a key the system does not take | `<path>: <section>.<key> is not a key of system '<system>'` |
| a required key is missing | `<path>: <section>.<key> is required for system '<system>'` |
| a value has the wrong JSON type, or is `null` | `<path>: <section>.<key> must be ...`, as in [JSON types](#json-types) |
| a callback argument that the system does not take is given | `<path>: <section> system '<system>' takes no argument <name>` |
| `connection` is missing for `postgres` | `<path>: <section> system 'postgres' needs the argument connection` |
| the connector rejects a value, such as `table_name: "orders"` for `unity_catalog` | the connector's `ValueError`, the same object, with one note |
| the system's extra is not installed | the connector's `ImportError`, the same object, with one note |
| `SCD2` only: `target.system` is not `bigquery`, `postgres` or `unity_catalog` | `<path>: target.system '<name>' is not one of: bigquery, postgres, unity_catalog` |
| `SCD2` only: the `scd2` section is missing | `<path>: missing section 'scd2'` |
| `SCD2` only: it is not an object | `<path>: section 'scd2' must be a JSON object` |
| `SCD2` only: a key not in the table | `<path>: scd2.<key> is not a key of section 'scd2'` |
| `SCD2` only: a required key is missing | `<path>: scd2.<key> is required` |
| `SCD2` only: `effective_time_column`, `valid_from_column` or `valid_to_column` is not a non-empty string, or is `null` | `<path>: scd2.<key> must be a non-empty string` |
| `SCD2` only: `key_columns` or `tracked_columns` is not a non-empty array of non-empty strings | `<path>: scd2.<key> must be a non-empty array of non-empty strings` |
| `SCD2` only: a column twice in one array | `<path>: scd2.<key> names the column '<name>' twice` |
| `SCD2` only: a column in two roles | `<path>: scd2.<role> and scd2.<later role> both name the column '<name>'` |
| an argument the callback does not take, such as `replace=True` for `AppendOnly` | Python's `TypeError` for an unexpected keyword argument |
| `read()` or `write()` fails | the error, unchanged, with no note, when the process runs |
| `state[key]` is missing | `KeyError`, whose `args` are `(key,)`, when the process runs; the writer is not called |

- `<names>` are the names valid for the section, sorted and separated by `, `: for a source
  `bigquery, disk, gcs, json_api, postgres, s3, unity_catalog, volume`, for a target
  `bigquery, disk, gcs, json_lines, postgres, s3, unity_catalog, volume`.
- The checks run in this order: the argument and the file, the JSON, the section, `system`, the
  keys in the order of the file, the required keys in the order of the tables above, the types in
  the order of the file, the callback arguments, and last the connector.
- `SCD2` runs these for its `target` section first. Then it checks the `scd2` section: that it is
  present and an object, its keys in the order of the file, its required keys (`key_columns`, then
  `effective_time_column`), the type of each value in the order of the file (for an array, its
  type, then that no column appears twice), and last that no column has two roles. The pairs of
  roles are checked in this order: `key_columns` with each later role, `effective_time_column` with
  each later role, `tracked_columns`, then `valid_from_column` with `valid_to_column`. The two
  names are written as the earlier role writes them.
- The errors that `SCD2` raises when the process runs, such as a missing column or a failed check,
  are in [Callbacks](callbacks.md#the-checks).
- The note is `while building the <section> of <path>, system '<system>'`, added with
  `add_note`. The error is not wrapped: its type, message and cause stay the connector's.
- A callback that fails to build never reaches `Process`, so its error writes no line of the run's
  log.

## Extras

The [extras](connectors.md#extras) stay as they are. A configuration that names a system whose
extra is not installed raises, when the callback is built, the `ImportError` that the connector
raises, which names the extra: `PostgresWriter needs psycopg: pip install 'ingestio[postgres]'`.
It follows the connector's rule of needing the package only when it would import it: an `s3`, `gcs`
or `volume` source given a `client` needs no extra, and a `bigquery` source without
`watermark_column` needs none either. So changing a job's system is an edit of its JSON and, when
the new system needs an extra, of its install.

## Example

A job that moves records from a PostgreSQL table into a JSON Lines file. Building `ReadSource`
opens nothing, so this runs without a database: the process here runs only the load, over records
placed in the state.

```python
import json
import tempfile
from pathlib import Path

from ingestio import AppendOnly, Process, ReadSource, Step
from ingestio.connector.all import PostgresConnection

with tempfile.TemporaryDirectory() as tmp:
    out = Path(tmp) / "bronze" / "orders.jsonl"
    config = Path(tmp) / "job.json"
    config.write_text(
        json.dumps({
            "source": {
                "system": "postgres",
                "table_name": "public.orders",
                "watermark_column": "updated_at",
            },
            "target": {"system": "json_lines", "path": str(out)},
            "primary_key": "id",
        }),
        encoding="utf-8",
    )

    # Credentials come from the job: the environment, or dbutils.secrets.get on Databricks.
    shop = PostgresConnection("pg.internal", "shop", "ingest", "s3cret")
    extract = ReadSource(config, connection=shop, watermark_value=41)  # builds, reads nothing
    load = AppendOnly(config)  # adds to the target; FullLoad(config) would replace it

    Process(
        [Step("load", [load])],
        state={"records": [{"id": 1}, {"id": 2}]},
    ).run()
    print(out.read_text(encoding="utf-8"))

    # A mistake in the file stops the job when the callback is built.
    config.write_text(json.dumps({"target": {"system": "mysql"}}), encoding="utf-8")
    try:
        AppendOnly(config)
    except ValueError as error:
        print(error)
```

A full job, with the process that reads from PostgreSQL and loads a Unity Catalog table. It needs
a Spark session and a PostgreSQL server.

<!-- example: skip -->
```python
import os
from datetime import datetime
from pathlib import Path

from ingestio import AppendOnly, Callback, CancelStepException, Process, ReadSource, Step
from ingestio.connector.all import PostgresConnection

CONFIG = Path(__file__).with_name("config.json")
# {
#   "source": {"system": "postgres", "table_name": "public.orders", "watermark_column": "updated_at"},
#   "target": {"system": "unity_catalog", "table_name": "main.bronze.orders"},
#   "primary_key": "id"
# }


class SkipEmptyBatch(Callback):
    """The job's own callback: nothing to load when the source returned no rows."""

    def run(self) -> None:
        if self.process.state["records"].isEmpty():
            raise CancelStepException


def last_watermark() -> datetime | None:
    """The job's own to write: the last watermark ingested, from the target or a control table.

    A timezone-aware datetime; None reads every row.
    """
    return None


# Credentials come from the environment; on Databricks, from dbutils.secrets.get.
shop = PostgresConnection(
    "pg.internal", "shop", os.environ["SHOP_USER"], os.environ["SHOP_PASSWORD"]
)

Process([
    Step("extract", [ReadSource(CONFIG, connection=shop, watermark_value=last_watermark())]),
    Step("load", [SkipEmptyBatch(), AppendOnly(CONFIG)]),
]).run()
```

Moving the target to BigQuery is an edit of the `target` section, plus `ingestio[bigquery]` in the
job's install, because the BigQuery writer needs that extra.

## Known limitations

- **One source and one target per file.** A job with two sources uses two files. `SCD2` adds one
  more: one dimension per file.
- **A job's own member named `scd2` is the library's.** `SCD2` reads it and rejects what it does not
  expect, so the job renames its member before placing `SCD2`. `source`, `target` and `scd2` are
  the reserved names.
- **The watermark is computed before the process is built.** `watermark_value` is an argument of
  the callback, which the job builds before the run. A watermark that a step of the same process
  would compute cannot reach the reader.
- **A `StructType` schema cannot be given.** A DDL string covers every Spark type; column
  metadata other than comments is lost.
- **A key that appears twice** in a section keeps its last value, silently, as JSON parsers do.
- **The file is read when the callback is built.** A long-lived process does not see later
  changes to the file.
- **No connector of the job's own.** Only the systems of the table can be named: a system the
  library does not integrate gets a connector contributed to the library.
