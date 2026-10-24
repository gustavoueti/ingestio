# 0015. Choose the reader and the writer from the job's configuration file

- **Status:** accepted, amended by [0016](0016-make-each-write-strategy-one-callback.md) and
  [0017](0017-read-a-library-callbacks-own-parameters-from-a-section-named-after-it.md)
- **Recorded:** 2026-10-23
- **Evidence:** [specs/config_connectors.md](../../specs/config_connectors.md), decisions 1 to 17,
  taken by the maintainer on 2026-10-23, and the reasons the maintainer gave the same day,
  including those for decision 5;
  `src/ingestio/callback/io.py`. It amends [the philosophy](../philosophy.md),
  [ADR 0003](0003-connectors-as-protocols.md) and
  [ADR 0013](0013-connector-configuration-convention.md). It is amended by
  [ADR 0016](0016-make-each-write-strategy-one-callback.md), which replaces `WriteTarget` with
  `AppendOnly` and `FullLoad`. It is amended by
  [ADR 0017](0017-read-a-library-callbacks-own-parameters-from-a-section-named-after-it.md), which
  adds the `scd2` section to the shape of the file. It is amended on 2026-10-24 by
  [specs/mongodb_reader.md](../../specs/mongodb_reader.md), which adds the `mongodb` system and
  widens the `connection` argument per system.

## Context

- Jobs read their JSON configuration in `main.py`, built the connectors from it and injected
  them into `ReadSource` and `WriteTarget`, the write callback of the time. Changing the source from disk to S3 meant changing
  the code of the job, not only its configuration.
- The philosophy ruled out registries and asked for connectors that users extend.
- [ADR 0013](0013-connector-configuration-convention.md) left the job with the mapping of
  configuration keys to constructor arguments, and recorded, without resolving it, that
  `table_name`, `path` and `volume` of one flat configuration name the source while the same
  argument of a writer names the target.

## Decision

- **The configuration file is the only way** the library's read and write callbacks reach a
  connector. `ReadSource(config, ...)` and the library's write callbacks, `AppendOnly(config, ...)`
  and `FullLoad(config, ...)`, take the path of the job's JSON configuration file, read it
  themselves and build the connector it names. They do not accept a connector instance. The change
  breaks the public API; the package is alpha. (This ADR was written for a single `WriteTarget`;
  [ADR 0016](0016-make-each-write-strategy-one-callback.md) replaced it with the two callbacks.)
- **The shape of the file.** One `source` section for `ReadSource` and one `target` section for
  each write callback, each naming its system with the key `system`. The names are the connector's
  class name in snake case without `Reader` or `Writer`: `json_api`, `json_lines`, `disk`,
  `postgres`, `s3`, `gcs`, `volume`, `unity_catalog`, `bigquery`, `mongodb` (a source only, added
  by [specs/mongodb_reader.md](../../specs/mongodb_reader.md)). The other keys of a section
  are the connector's constructor arguments of the same name. (Amended by
  [ADR 0017](0017-read-a-library-callbacks-own-parameters-from-a-section-named-after-it.md): a
  third section, `scd2`, holds the parameters of `SCD2`, which only that callback reads. The top
  level stays the job's except for the names `source`, `target` and `scd2`.)
- **What does not belong in the JSON** reaches the callback as arguments: credentials, such as
  a `PostgresConnection` the job builds from its own secrets, SDK clients (`client=`), and
  per-run values such as `watermark_value`. The library still never reads secrets.
- **The closed table is the one registry the library has.** The mapping from system names to the
  connectors the library ships is private to the library, holds only shipped connectors, and
  nothing outside the library can add to it: no mapping argument, no import path in the
  configuration, no entry point.
- **Users extend callbacks, not connectors.** A system the library does not integrate gets a
  connector contributed to the library, or a feature request. The connector classes,
  `Reader` and `Writer` stay public, so that a job's own callback can still build a shipped
  connector for a strategy the library's callbacks do not offer.
- **The extras stay as they are.** A configuration that names a system needs the extra of that
  system when its connector would import the package.

## Rationale

The maintainer's request: "I want the jobs to only `import ingestio` and from there use the
callbacks. Which read connector (source system)? Which write connector (target system)? That
goes in the config: the callback that does the extraction reads it, and the callback that
writes reads which system it writes to, so it uses the right connector."

The maintainer gave these reasons on 2026-10-23:

- **The configuration as the only way:** simplicity. The job's code should be as clean as
  possible: it declares the source and the target in its configuration and places callbacks.
- **The callback reads the file:** `main.py` does not have to parse anything.
- **Only the connectors the library ships:** a system that is missing comes in through a
  contribution to the library or a feature request, not through the job's code.

The credentials stay out of the file for the reason of
[ADR 0013](0013-connector-configuration-convention.md): on Databricks they come from
`dbutils.secrets.get`, which the library never imports.

The extras stay as they are (decision 5). They appear only in the install line: the job's code
imports `ingestio` either way. Putting every integration's package in the core dependencies was
considered and dropped: it would install them in every job, including the jobs that never use
those systems, with the risk of version conflicts with what the runtime already ships.

The table is closed, and not opened by a mapping argument or an import path in the configuration,
for the reason of decision 4 above: a system that is missing comes in through a contribution to
the library or a feature request, not through the job's code.

## Consequences

- **The philosophy changes in three sentences.** The library ships every connector, and the read
  and write callbacks build the matching one from the configuration. "Registries" become "open
  registries", and the closed table is the one mapping from names to classes the library has.
  Users no longer extend connectors.
- **ADR 0003 and ADR 0013 are amended.** A connector is no longer injected into the library's
  read and write callbacks, and the library, not `main.py`, reads the configuration. The
  connector itself still reads no file, environment variable or secret store, and has no
  `from_config`.
- **The ambiguity of `table_name` ends in a configuration.** Each section names its own system,
  so the `target` section's `table_name`, `path` or `volume` is the target's. It remains only
  for a job's own callback that builds a writer itself.
- **A job that reads or writes through `ReadSource`, `AppendOnly` or `FullLoad` has a JSON configuration
  file and uses a shipped connector.** The job still composes its process in code: the steps,
  their order and its own callbacks.
- **Changing a system is an edit of the JSON** and, when the new system needs an extra, of the
  job's install.
- **A system the library lacks needs a contributed connector.** It becomes one more row of the
  table, through a spec of its own.
- **`ReadSource`, `AppendOnly` and `FullLoad` stay essential** ([ADR 0011](0011-callbacks-as-stages-with-criticality.md)):
  every error propagates, and configuration errors are raised when the callback is built, before
  anything runs.
- **The `connection` argument takes the connection value of the system that needs one**
  (amendment of 2026-10-24, [spec](../../specs/mongodb_reader.md)). `ReadSource` takes a
  `PostgresConnection` for a `postgres` source and a `MongoDBConnection` for a `mongodb` source,
  one existing argument widened per system. `AppendOnly`, `FullLoad` and `SCD2` keep
  `PostgresConnection`, because `mongodb` is not a target: there is no MongoDB writer, a scope
  decision whose reason is `TODO(maintainer)`. A later system with credentials, such as backlog
  idea 8, would follow the same pattern. Why the existing argument widens rather than a new
  argument being added: `TODO(maintainer)`. The technical reason the maintainer gave for the value
  itself: the job reads the connection string from a secret store at run time and hands it to the
  connector, so it never sits in the configuration.
- **The callbacks check the type of `connection`** when they are built, for every system that
  takes one, in `ReadSource`, `AppendOnly`, `FullLoad` and `SCD2`. A connection of another
  system's class raises a `ValueError` with the file's path and the system in the message, before
  anything runs, instead of an `AttributeError` when the process runs. The check lives in the
  callbacks, not in the connectors, which stay unchanged. (The spec gives this reason as the
  description of the option the maintainer chose, marked for the maintainer's review.)
- **Nothing closes the door to streaming** ([ADR 0012](0012-streaming-deferred.md)). The
  `Reader` and `Writer` protocols and their open payload do not change, and a streaming
  connector would be one more shipped system in the table, with keys of its own.
