# Spec: MySQL reader and writer connectors

- **Status:** validated
- **Slug:** `mysql_connectors`. The spec is `specs/mysql_connectors.md` and its tests are
  `tests/test_mysql_connectors.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md),
  [ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md),
  [ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md),
  [specs/postgres_connectors.md](postgres_connectors.md),
  [specs/mongodb_reader.md](mongodb_reader.md),
  [specs/config_connectors.md](config_connectors.md),
  [specs/append_only_callback.md](append_only_callback.md),
  [specs/scd2_callback.md](scd2_callback.md),
  [backlog idea 8](../docs/backlog.md#8-mysql-reader-and-writer-with-cdc),
  [specs/mysql_cdc.md](mysql_cdc.md), the CDC reader,
  [Connectors](../docs/concepts/connectors.md),
  [Configuration](../docs/concepts/configuration.md),
  [Callbacks](../docs/concepts/callbacks.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

Two connectors for MySQL, built on Spark's JDBC data source with MySQL Connector/J, and the value
that says how to reach the database. `MySQLReader` returns a Spark DataFrame over one table: all
of it, or, with a watermark column, only the rows above the job's last watermark, filtered inside
MySQL over a slice that `read()` freezes with an upper bound, as `PostgresReader` does.
`MySQLWriter` appends a DataFrame to a table, or replaces the table's rows with it, atomically:
Spark loads a staging table in parallel, then one transaction on the Spark driver, run with
PyMySQL, executes `DELETE FROM <target>` when it replaces and `INSERT … SELECT` from the staging
table, and commits; the staging table is dropped afterwards. `MySQLConnection` holds the host, the
database, the credentials, the port and the TLS mode, and translates them for Connector/J and for
PyMySQL; the CDC reader of a later spec takes its PyMySQL arguments from it.

The job names the system `mysql` in its configuration, as a source and as a target. The
`connection` argument of `ReadSource`, `AppendOnly`, `FullLoad` and `SCD2` widens to take a
`MySQLConnection`, and the callbacks check its type. PyMySQL goes into a new extra,
`ingestio[mysql]`, which only the writer needs.

The maintainer answered the 20 open questions of the first draft on 2026-10-24, each with the
recommended option ([Decisions](#decisions) 24 to 43), and accepted the decisions this spec
proposed (13 to 23). A rule or a criterion that rests on one of these decisions names it.

## Motivation

- **A MySQL source and target.** Backlog idea 8 asks for a MySQL connector and for change data
  capture. On 2026-10-24 the maintainer split it ([Decisions](#decisions) 1 to 3): this spec gives
  the reader, the writer and the connection; a later spec, `mysql_cdc`, gives a CDC reader of the
  binary log that reuses `MySQLConnection`; applying change events to a target is a callback with a
  spec of its own, after that.
- **Incremental reads, filtered in the source.** Jobs ingest tables by a watermark column such as
  `updated_at`, and [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md)
  requires a reader to filter inside the source.
- **Laziness.** A DataFrame read over JDBC queries MySQL again at every action. Without an upper
  bound, a job that counts the rows, writes them and computes its next watermark from the same
  DataFrame can see different rows each time, and record a watermark above rows it never wrote.
  The PostgreSQL reader captures a bound for this reason (its decision 10).
- **Atomic writes.** Spark's JDBC writer commits one transaction per partition, so a failed write
  leaves part of the data in the target. `PostgresWriter` makes a write atomic with a staging table
  and one transaction that runs `TRUNCATE`; in MySQL `TRUNCATE TABLE`, like `CREATE TABLE` and
  `DROP TABLE`, commits the open transaction implicitly (M1), so the replace runs a `DELETE`
  instead (decision 3).
- **Credentials.** A job reads the user and the password from a secret store at run time,
  `dbutils.secrets.get` on Databricks. The library never reads a secret.
- **SQL modes.** A MySQL server's `sql_mode` changes how a string literal (M3) and a double-quoted
  name (M2) are read, and a job does not control the server's mode. Every statement the connectors
  write must mean the same in every mode.

## Fit with the philosophy

- **Concept: connector.** `MySQLReader` satisfies `Reader[DataFrame]` and `MySQLWriter`
  satisfies `Writer[DataFrame]`, structurally, with no base class and no registration
  ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)). `ReadSource`, `AppendOnly`,
  `FullLoad` and `SCD2` build them from the job's configuration
  ([ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md)), through one
  more row of the closed table, which stays closed.
- **No third concept.** `MySQLConnection` is a value, like `PostgresConnection`: it groups what
  reaching one database needs, opens nothing and performs no action. The CDC reader of the later
  spec takes its PyMySQL arguments from the same value, through a private method; nothing public is
  added for it.
- **The connectors decide no strategy.** `write(data, *, replace=False)` offers the two primitive
  ways MySQL has to put a batch into a table, `INSERT` and `DELETE` followed by `INSERT`, and picks
  neither: `AppendOnly` appends and `FullLoad` replaces
  ([ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md)). The staging table
  and the transaction are how the writer talks to MySQL so that a write is all-or-nothing, a
  mechanism, not a strategy. Applying change events is a write strategy, a callback with a spec of
  its own (decision 1). Whether a run is incremental, and from which value, is the job's decision;
  the reader turns the column and the value into SQL.
- **The writer creates nothing on the target.** A missing target fails, and the staging table is
  the writer's own mechanism, created and dropped within one call
  ([ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md)).
- **Essential by default.** The connectors catch nothing to carry on: every failure propagates out
  of `read()` or `write()`, and fails the callback that called them.
- **Simplicity.** Three small classes in one module, two rows of the table, one widened annotation
  per callback. No registry, no new protocol, no new callback.
- **Compose, do not impose.** The job builds the `MySQLConnection` from its own secrets. The
  connectors read no file, environment variable or secret store. A job's own callback can still
  build a `MySQLReader` or a `MySQLWriter` itself.
- **No dependencies in the core.** PyMySQL goes into the `mysql` extra, needed by the writer only.
  pyspark and MySQL Connector/J, a JAR, are prerequisites that the runtime provides. `ingestio` and
  `ingestio.connector.all` still import without them.
- **Streaming.** Nothing here closes the door; see [ADRs](#adrs).

## Scope

In scope:

- `MySQLConnection`: its checks, its `repr`, and its translation for Connector/J (the executors)
  and for PyMySQL (the driver), built in one place so that the later CDC reader reuses it;
- `MySQLReader`: full reads, and watermark reads filtered inside MySQL over a slice that `read()`
  freezes with an upper bound;
- `MySQLWriter`: atomic append (`write(data)`) and atomic replace (`write(data, replace=True)`),
  through a staging table and one driver-side transaction;
- quoting of names, and literals that mean the same in every `sql_mode`;
- the time zone of the JDBC sessions, so that `TIMESTAMP` watermarks and values stay consistent;
- the `mysql` system, as a source and as a target, its keys, and `connection` widened to
  `MySQLConnection` in the four callbacks, with its type check;
- `mysql` as an `SCD2` target (decision 40);
- the `mysql` extra;
- the test doubles, and the opt-in integration tests against a real MySQL server.

Out of scope:

- **the CDC reader of the binary log**, with `python-mysql-replication`: the later spec
  `specs/mysql_cdc.md` (decision 1). This spec adds nothing public for it; it only builds the
  PyMySQL arguments of `MySQLConnection` in one place that the CDC reader can reuse;
- **the callback that applies change events to a target**: a spec of its own, after the CDC reader
  (decision 1);
- upsert, merge, delete or any operation besides append and replace;
- creating, altering or dropping the target (any DDL on the target);
- custom SQL queries, column projection and extra filters in the reader; automatic bounds for
  parallel reads;
- CA files, client certificates and other TLS material in `MySQLConnection` (decision 29); timeouts,
  connection attributes, several hosts, Unix sockets;
- computing, persisting or advancing the watermark, and exposing the bound that `read()` captured;
- MariaDB (decision 41), Lakehouse Federation, Databricks serverless compute, engines other than Spark;
- retries, row counts, connection pooling, log lines, tooling to clean up orphaned staging tables;
- changes to CI, `pyproject.toml`, `uv.lock` and `README.md`, which belong to the maintainer and
  the main session (see [Where it lives](#where-it-lives)).

## Public API

New, in the new module `ingestio/connector/mysql.py` (decision 26), exported from `ingestio.connector.all`
only (not from `ingestio`):

```python
from ingestio.connector.all import MySQLConnection, MySQLReader, MySQLWriter
```

```python
@dataclass(frozen=True, repr=False)
class MySQLConnection:
    """Where and how to reach one MySQL database. Opens nothing by itself.

    Args:
        host: Server host name or address.
        database: Database name. An unqualified table name resolves to it.
        user: User to connect as.
        password: The user's password. Never shown in `repr`.
        port: Server port.
        ssl_mode: TLS mode, as MySQL Connector/J names it: `DISABLED`, `PREFERRED`, `REQUIRED`,
            `VERIFY_CA` or `VERIFY_IDENTITY`. `None` leaves both clients' default, which is
            `PREFERRED`.
    """

    host: str
    database: str
    user: str
    password: str
    _: KW_ONLY
    port: int = 3306
    ssl_mode: str | None = None
```

```python
class MySQLReader:
    """Reads a MySQL table into a Spark DataFrame, filtering inside MySQL.

    Args:
        table_name: Source table, `table` or `database.table`. The configuration key of the same
            name.
        connection: Where the table lives, with its credentials.
        watermark_column: Column that orders changes. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only rows above it are read.
            Requires `watermark_column`.
        options: Spark JDBC options and Connector/J properties passed through, such as
            `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`, `fetchsize` or
            `sessionInitStatement`. They override the connector's defaults.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
        connection: MySQLConnection,
        *,
        watermark_column: str | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...

    def read(self) -> DataFrame:
        """Return a DataFrame over the rows the job needs."""
```

```python
class MySQLWriter:
    """Writes a Spark DataFrame into a MySQL table, atomically.

    Args:
        table_name: Table to write into, `table` or `database.table`. It must exist.
        connection: Where the table lives, with its credentials.
        options: Spark JDBC write options and Connector/J properties passed through, such as
            `numPartitions` or `batchsize`. They override the connector's defaults.
    """

    def __init__(
        self,
        table_name: str,
        connection: MySQLConnection,
        *,
        options: Mapping[str, str | int | float | bool] | None = None,
    ) -> None: ...

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` to the target, in one transaction.

        Args:
            data: The rows to write.
            replace: Remove every row of the target first, in the same transaction, so that the
                target ends up holding exactly the rows of `data`. Appends when `False`.
        """
```

- `DataFrame` and `SparkSession` are `pyspark.sql.DataFrame` and `pyspark.sql.SparkSession`.
  `Decimal` is `decimal.Decimal`; `date` is `datetime.date`, and `datetime.datetime`, its
  subclass, is accepted too. `Mapping` is `collections.abc.Mapping`. `KW_ONLY` and `dataclass`
  are from `dataclasses`.
- `MySQLConnection` is immutable, and its arguments are readable as attributes of the same names.
  The attributes of `MySQLReader` and `MySQLWriter` are not documented, so they are not public
  API. Their `repr` is specified under [Behaviour](#mysqlconnection).
- `table_name` and `connection` are required, so they are positional, in this order, as on the
  PostgreSQL connectors. Every other argument is keyword-only.
- `MySQLWriter` has one write method, `write`, and no `replace()` method; `replace` is
  keyword-only with a default, so `write` still satisfies `Writer[DataFrame]`.

**Changed: the `connection` argument of the four callbacks**, exported by `ingestio` and
`ingestio.callback.all` as before. Only the annotation and the docstring change:

```python
class ReadSource(Callback):
    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        into: str = "records",
        connection: PostgresConnection | MongoDBConnection | MySQLConnection | None = None,
        client: object | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...


class AppendOnly(Callback):  # FullLoad has the same signature
    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        key: str = "records",
        connection: PostgresConnection | MySQLConnection | None = None,
        client: object | None = None,
    ) -> None: ...


class SCD2(Callback):  # (decision 40)
    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        key: str = "records",
        connection: PostgresConnection | MySQLConnection | None = None,
        client: object | None = None,
    ) -> None: ...
```

**Changed: the configuration** (decision 5):

- the system `mysql`, a source that builds `MySQLReader` and a target that builds `MySQLWriter`;
- source keys: `table_name` (required), `watermark_column` and `options`; target keys:
  `table_name` (required) and `options`;
- callback arguments: `connection`, a `MySQLConnection`, required; `watermark_value` and `spark`
  optional for the source; `client` not taken;
- `mysql` is a target of `SCD2` (decision 40);
- the lists of systems in the messages `<path>: <section>.system '<name>' is not one of: <names>`
  gain `mysql`, and so does `SCD2`'s narrowed list (decision 40).

**New installable extra:** `ingestio[mysql]` (decision 25).

**Unchanged:** `Reader`, `Writer`, every other connector, `PostgresConnection`,
`MongoDBConnection`, and everything exported by `ingestio` other than the annotations above.

### Usage sketch

A sketch of a Databricks job, not a runnable example: `dbutils` and `last_watermark()` are the
job's. The configuration, `orders.json`:

```json
{
  "source": {"system": "mysql", "table_name": "shop.orders", "watermark_column": "updated_at"},
  "target": {"system": "unity_catalog", "table_name": "main.bronze.orders"}
}
```

```python
from ingestio import AppendOnly, Process, ReadSource, Step
from ingestio.connector.all import MySQLConnection

shop = MySQLConnection(
    "mysql.internal",
    "shop",
    dbutils.secrets.get("mysql", "user"),  # the job reads its secrets; the library never does
    dbutils.secrets.get("mysql", "password"),
    ssl_mode="VERIFY_IDENTITY",
)

Process([
    Step("extract", [ReadSource("orders.json", connection=shop, watermark_value=last_watermark())]),
    Step("load", [AppendOnly("orders.json")]),
]).run()
```

A full load into MySQL, from a file whose `target` is
`{"system": "mysql", "table_name": "reporting.daily_sales"}`, places
`FullLoad("daily_sales.json", connection=shop)` instead of `AppendOnly`.

## Behaviour

### Evidence

The main session read the sources below on 2026-10-24 and gave the planner the facts S1 to M6;
the spec cites them by number. The same day it read the sources again for J6 and P5 to P7, which
confirm three claims of the first draft (K12, K15 and the PyMySQL part of K7) and add the typing of
PyMySQL. The facts K1 to K16 that remain are the planner's own knowledge, **not verified**: each is
to be confirmed, in the source or the manual it names, or by the integration tests of this spec,
and is cited as "(K<n>, to be confirmed)" where a rule rests on it.

**Spark** (minimum supported pyspark 3.5, per `pyproject.toml`), file
`sql/core/src/main/scala/org/apache/spark/sql/jdbc/MySQLDialect.scala`:

- **S1** (v3.5.0 and v4.2.0): `MySQLDialect.canHandle` matches URLs starting with `jdbc:mysql`
  (lower-cased).
- **S2**: `quoteIdentifier` at v3.5.0 returns `` `name` `` without doubling backticks inside the
  name; at v4.2.0 it doubles them. So the connector must quote identifiers itself rather than rely
  on the dialect, as `PostgresReader` already does.
- **S3**: the dialect's type mapping changed between the two tags: at v4.2.0 `DATETIME` maps
  through `getTimestampType` (timestamp without time zone, unless the legacy conf
  `legacyMySqlTimestampNTZMappingEnabled`), `TIMESTAMP` to `TimestampType`, unsigned
  `TINYINT`/`SMALLINT` widen, `BIT(n>1)` maps to `BinaryType` unless
  `legacyMySqlBitArrayMappingEnabled`; v3.5.0 maps `BIT(n>1)` to `LongType`. The Spark schema of a
  MySQL read therefore depends on the Spark version.
- **S4**: neither tag sets a fetch size for MySQL in the dialect.

**MySQL Connector/J** (tag 26.10.0, the latest;
`src/main/core-api/java/com/mysql/cj/conf/PropertyDefinitions.java` and
`src/main/resources/com/mysql/cj/LocalizedErrorMessages.properties`):

- **J1**: `sslMode` (since 8.0.13), default `PREFERRED`; values `DISABLED`, `PREFERRED`,
  `REQUIRED`, `VERIFY_CA`, `VERIFY_IDENTITY`. It replaced the legacy `useSSL`, `requireSSL` and
  `verifyServerCertificate`. CA material for `VERIFY_CA` comes from a Java trust store
  (`trustCertificateKeyStoreUrl`).
- **J2**: `useCursorFetch`, default false: only when it is true and a fetch size above 0 is set
  does Connector/J fetch with a server-side cursor; otherwise a result set is read whole into
  memory, or streamed only with the fetch size `Integer.MIN_VALUE`. So Spark's `fetchsize` alone
  has no effect on MySQL, unlike `PostgresReader`'s default `fetchsize` of 10000.
- **J3**: `rewriteBatchedStatements`, default false: rewrites batched prepared `INSERT`s into
  multi-values statements, the counterpart of pgJDBC's `reWriteBatchedInserts`, which
  `PostgresWriter` sets.
- **J4**: `connectionTimeZone` (default `LOCAL`, the JVM's zone), `forceConnectionTimeZoneToSession`
  (default false, since 8.0.23) and `preserveInstants` (default true, since 8.0.23) govern how
  `TIMESTAMP` values cross between the JVM and the session `time_zone`.
- **J5**: `sessionVariables` sets session variables on connect. The driver class is
  `com.mysql.cj.jdbc.Driver`, and the URL `jdbc:mysql://host:port/database`.
- **J6** (tag 26.10.0, `src/main/core-api/java/com/mysql/cj/conf/ConnectionUrlParser.java`, line
  159): the URL's path, the database name, is decoded with `java.net.URLDecoder.decode(text, UTF-8)`
  and then trimmed with `.trim()`. `URLDecoder` also turns `+` into a space, so the database name
  must be percent-encoded with `+` written `%2B`, which `urllib.parse.quote(name, safe="")` does.
  Leading and trailing spaces of a database name are trimmed.

**PyMySQL** (MIT; tags v1.1.0 to v1.2.3, the latest, released 2026-09-17;
`pymysql/connections.py`):

- **P1**: `python-mysql-replication` (PyPI `mysql-replication`, Apache-2.0, latest 1.0.17)
  declares `install_requires = ["packaging", "pymysql>=1.1.0"]` (`setup.py` at tag 1.0.17).
  Choosing PyMySQL as this spec's client keeps one MySQL client in the extra once the CDC spec adds
  `mysql-replication`; mysql-connector-python would add a second one.
- **P2**: `connect()` defaults to `autocommit=False`; `begin()`, `commit()` and `rollback()` exist
  at both tags. Its arguments include `host`, `port` (default 3306), `user`, `password`,
  `database`, `init_command`, `charset`, `ssl`, `ssl_ca`, `ssl_cert`, `ssl_key`, `ssl_disabled`,
  `ssl_verify_cert` and `ssl_verify_identity`.
- **P3**: the TLS default changed. At v1.1.0 to v1.1.3, with no ssl argument, PyMySQL connects
  without TLS; since v1.2.0 (released 2026-05-19, PR #1234 "implement PREFERRED/REQUIRED SSL mode
  behavior") it tries TLS and falls back (PREFERRED), and any explicit ssl argument makes TLS
  required. At v1.1.0, `ssl={}` is falsy and enables nothing, and `ssl_verify_cert=False` alone
  enables nothing either; an `ssl.SSLContext` passed as `ssl` is used as given. So mapping one
  `ssl_mode` to both Connector/J (J1) and PyMySQL needs an explicit translation, and the CA
  material differs: a Java trust store for one, a PEM file for the other.
- **P4**: `python-mysql-replication` 1.0.16 or later is needed for MySQL 8.4: it falls back from
  `SHOW BINARY LOG STATUS` to `SHOW MASTER STATUS` only from 1.0.16, and MySQL 8.4 removed
  `SHOW MASTER STATUS`. Context for the later spec only.
- **P5** (v1.2.3, `pymysql/_auth.py`; `pyproject.toml` at v1.2.0 and v1.2.3): in
  `caching_sha2_password_auth`, on full authentication (`n == 4`) over a secure connection
  (`conn._secure`), the plain password is sent and `cryptography` is not needed. Without TLS, the
  client requests the server's public key and calls `sha2_rsa_encrypt`, which raises
  `RuntimeError("'cryptography' package is required for sha256_password or caching_sha2_password auth")`
  when `cryptography` is missing. `pyproject.toml` declares the extra
  `rsa = ["cryptography>=46.0.7"]`, and also `ed25519 = ["PyNaCl>=1.6.2"]`.
- **P6** (v1.2.3, `pymysql/cursors.py`): `mogrify(query, args=None)` returns `query` unchanged when
  `args is None`, so a statement executed with no arguments is sent as written.
- **P7** (v1.2.3, the `pymysql/` package; typeshed, `stubs/PyMySQL/METADATA.toml`): PyMySQL ships
  no `py.typed` marker. typeshed has `stubs/PyMySQL`, with `version = "1.2.*"`, published as
  `types-PyMySQL`.

**MySQL server** (the Reference Manual, not source code):

- **M1**: `CREATE TABLE`, `DROP TABLE`, `TRUNCATE TABLE` and `RENAME TABLE` commit the open
  transaction implicitly ("Statements That Cause an Implicit Commit"). `CREATE TEMPORARY TABLE`
  does not, but a temporary table is visible only to its own session, so Spark's executors cannot
  load one.
- **M2**: backticks always quote identifiers; double quotes do so only under the `sql_mode`
  `ANSI_QUOTES`.
- **M3**: in a string literal, backslash is an escape character unless `sql_mode` contains
  `NO_BACKSLASH_ESCAPES`, so doubling `'` is not enough to make a literal safe in every mode; a
  hex string literal (`X'…'`, optionally with an introducer such as `_utf8mb4`) means the same in
  every mode. This bears on the watermark predicate, which `PostgresReader` builds as SQL text.
- **M4**: `TIMESTAMP` values are converted between UTC and the session `time_zone`; `DATETIME`
  values are not. A bound rendered as text with `CAST(MAX(col) AS CHAR)` and compared back as a
  literal round-trips only when both queries run in the same session time zone, and is ambiguous
  in a zone with daylight saving time during the repeated hour.
- **M5**: `CREATE TABLE … LIKE` copies column definitions, indexes and generated columns, not
  foreign keys; `CREATE TABLE … AS SELECT … WHERE FALSE` (or `LIMIT 0`) copies only column types.
- **M6** (the main session's knowledge, to be confirmed): MySQL 8.0 reached end of life in April
  2026; the supported lines are 8.4 LTS and 9.x. MariaDB uses its own JDBC URL (`jdbc:mariadb:`),
  which S1 does not match.

**The planner's knowledge, to be confirmed:**

- **K1**: MySQL compares a string with a number as double-precision floating-point numbers
  (Reference Manual, "Type Conversion in Expression Evaluation"), so a string literal compared
  with a `BIGINT` column is not exact above 2^53, nor with a `DECIMAL` column beyond 15 to 17
  significant digits.
- **K2**: when one side of a comparison is a `DATETIME` or `TIMESTAMP` column and the other a
  constant, MySQL converts the constant to a temporal value before comparing; a hex string with an
  `_utf8mb4` introducer is such a constant.
- **K3**: with `sql_require_primary_key=ON` (MySQL 8.0.13 and later), a statement that creates a
  table without a primary key fails, `CREATE TABLE … AS SELECT` included; Group Replication
  (InnoDB Cluster) rejects DML on a table without a primary key.
- **K4**: a server-side cursor (J2) is materialised by the server in an internal temporary table
  before the first row is fetched.
- **K5**: in InnoDB, rows deleted by an uncommitted transaction stay visible to the consistent
  (non-locking) reads of other transactions until it commits, and the `DELETE` holds row locks
  until then; a `DELETE` fires the `ON DELETE` actions of the foreign keys that reference the table
  (`CASCADE` deletes the child rows, `SET NULL` sets their columns to `NULL`) and the table's
  `DELETE` triggers, once per row; foreign keys are checked at each row, not at commit, so deleting
  a referenced row under `RESTRICT` or `NO ACTION` fails even when the same key is inserted again
  later in the transaction.
- **K6**: creating a table grants its creator nothing on it: inserting into, reading and dropping
  the staging table need `INSERT`, `SELECT` and `DROP` on the database, besides `CREATE`.
- **K7**: `caching_sha2_password` is the default authentication plugin of MySQL 8.4, where
  `mysql_native_password` is disabled by default. (The PyMySQL part of the first draft's K7 is
  confirmed: P5.)
- **K8**: `ssl.create_default_context()` loads the system's default CA certificates, and OpenSSL's
  default verify paths honour the environment variables `SSL_CERT_FILE` and `SSL_CERT_DIR`
  (Python's documentation of `ssl.get_default_verify_paths`).
- **K9**: Connector/J's Maven coordinate is `com.mysql:mysql-connector-j` since 8.0.31, and
  `mysql:mysql-connector-java` before.
- **K10**: one transaction is limited by `max_binlog_cache_size` and, under Group Replication, by
  `group_replication_transaction_size_limit`; past them it fails and rolls back.
- **K11**: with the binary log in `ROW` format, a `DELETE` and an `INSERT … SELECT` write one row
  event per row, and the staging table's rows reach the binary log and the replicas too.
- **K12**: confirmed; see P6.
- **K13**: a MySQL identifier is at most 64 characters long; inside a backtick-quoted identifier a
  backtick is written twice (Reference Manual, "Schema Object Names").
- **K14**: database and table names are case-sensitive or not depending on `lower_case_table_names`
  and the server's file system; column names are not case-sensitive.
- **K15**: confirmed; see J6.
- **K16**: since MySQL 8.0.19 a datetime literal may carry a time zone offset, such as
  `'2026-10-01 08:30:00+00:00'`.

### `MySQLConnection`

`MySQLConnection` validates its arguments when it is built, in this order; the first that fails
raises `ValueError`:

| Argument | Rule | Message |
|---|---|---|
| `host` | not empty | `host must not be empty` |
| `database` | not empty | `database must not be empty` |
| `user` | not empty | `user must not be empty` |
| `port` | from 1 to 65535 | `port must be between 1 and 65535` |
| `ssl_mode` | `None`, `DISABLED`, `PREFERRED`, `REQUIRED`, `VERIFY_CA` or `VERIFY_IDENTITY`, exactly, in uppercase (J1) | `unsupported ssl_mode '<value>'` |

The password is not validated. Building imports nothing and opens nothing.

**Connector/J, on the executors** (decisions 27 and 29):

- the option `url` is `jdbc:mysql://<host>:<port>/<database>` (J5). A host that contains `:` (an
  IPv6 address) is wrapped in brackets. The database name is percent-encoded with
  `urllib.parse.quote(database, safe="")`, which writes `+` as `%2B`, as Connector/J's decoding of
  the path needs (J6). Connector/J trims the decoded name (J6), so a database name with leading or
  trailing spaces does not reach the server as written;
- the user and the password never go into the URL: they are the options `user` and `password`;
- the option `driver` is `com.mysql.cj.jdbc.Driver` (J5);
- when `ssl_mode` is set, the option `sslMode`, in exactly this case, has its value (J1).

**PyMySQL, on the driver** (decision 29). The *connect arguments* are the keyword arguments of
`pymysql.connect`, built by `MySQLConnection` in one private place, so that the writer uses them and
the CDC reader of the later spec can hand them to `BinLogStreamReader(connection_settings=...)`,
which calls `pymysql.connect(**connection_settings)`:

- `host`, `port`, `user`, `password` and `database`, from the arguments of the same names;
- `charset="utf8mb4"` (decision 16);
- the TLS arguments of the table below, and no other.

| `ssl_mode` | `sslMode` option | PyMySQL arguments added |
|---|---|---|
| `None` | not sent | none: PyMySQL's default, `PREFERRED` from v1.2.0 (P3) |
| `DISABLED` | `DISABLED` | `ssl_disabled=True` |
| `PREFERRED` | `PREFERRED` | none: PyMySQL's default from v1.2.0 (P3) |
| `REQUIRED` | `REQUIRED` | `ssl`: a new `ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)` whose `check_hostname` is `False` and `verify_mode` is `ssl.CERT_NONE` |
| `VERIFY_CA` | `VERIFY_CA` | `ssl`: a new `ssl.create_default_context()` whose `check_hostname` is set to `False`; its `verify_mode` stays `ssl.CERT_REQUIRED` |
| `VERIFY_IDENTITY` | `VERIFY_IDENTITY` | `ssl`: a new `ssl.create_default_context()`, unchanged: `check_hostname` is `True` and `verify_mode` is `ssl.CERT_REQUIRED` |

- An `ssl.SSLContext` passed as `ssl` is used as given (P3), so the PyMySQL side does what the
  mode says at every supported PyMySQL release, and `None` and `PREFERRED` mean the same on both
  clients from PyMySQL 1.2.0 (decision 25).
- **CA material** is each client's default trust store: the JVM's for Connector/J, the system's for
  Python (K8, to be confirmed). A job whose server uses a private CA installs it in both, or gives
  Connector/J a trust store through `options` (`trustCertificateKeyStoreUrl`, J1) and Python the
  environment variable `SSL_CERT_FILE` (K8). `MySQLConnection` takes no CA file (decision 29).
- The SSL context is created anew each time the connect arguments are built, when a connection is
  opened, never when `MySQLConnection` is built.

**`repr`** shows what identifies the database or the table, and never the password; `str` is the
same:

- `MySQLConnection(host='db.internal', port=3306, database='shop', user='ingest')`
- `MySQLReader(table_name='shop.orders', host='db.internal', database='shop')`
- `MySQLWriter(table_name='shop.orders', host='db.internal', database='shop')`

### Table and column names

- `table_name`, on both connectors, has the form `table` or `database.table` (decision 28): MySQL's schema
  is the database. The connector splits it on `.`. Anything other than one or two non-empty parts
  raises `ValueError("table_name must be 'table' or 'database.table'")` when the connector is
  built. A table name without a database resolves to the connection's database.
- Every name the connectors write into SQL (the table's parts, every column, the watermark column,
  the staging table) is quoted by the connector: wrapped in backticks, each backtick inside doubled
  (M2, K13). So `shop.orders` becomes `` `shop`.`orders` ``. No name is ever interpolated unquoted.
  Backticks quote in every `sql_mode` (M2); the connector never quotes with double quotes.
- Whether a name's case matters is the server's (K14, to be confirmed): the connector quotes the
  name as given and changes nothing.
- Spark writes some SQL of its own, the column list of its `SELECT` and of its `INSERT`, quoting
  the DataFrame's column names with the dialect's `quoteIdentifier`. At Spark 3.5.0 it does not
  double a backtick inside a name (S2), so a column whose name holds a backtick fails the read or
  the load on Spark 3.5; see [Known limitations](#known-limitations).

### Reading

`read()` returns a `pyspark.sql.DataFrame` built with `spark.read.format("jdbc")`. The executors
fetch the rows when an action runs on it. Apart from Spark's own schema queries, `read()` runs at
most the bound query described below.

**Spark session.** The reader uses `spark` when it is given. Otherwise it calls
`pyspark.sql.SparkSession.active()`, when `read()` is called, never when the reader is built. The
reader never creates a session; when none is active, PySpark's error propagates.

**Options of the data load**, in this order of precedence, lowest first:

- the base options of the connection: `url`, `user`, `password`, `driver`, and `sslMode` when it
  is set;
- `dbtable`, described below;
- the read defaults (decision 30): `useCursorFetch` = `true` and `fetchsize` = `10000`. Connector/J then
  fetches 10000 rows at a time through a server-side cursor; without them a partition is read
  whole into the executor's memory (J2, S4);
- the time-zone options (decision 35): `connectionTimeZone` = `UTC` and
  `forceConnectionTimeZoneToSession` = `true`, so that every JDBC session runs in UTC and
  Connector/J knows it (J4); see [Time zones](#time-zones);
- the user's `options`. A user option replaces the connector's option of the same name, with names
  compared case-insensitively as Spark compares them; only the user's key is sent.

Spark hands the options it does not know to Connector/J as driver properties, with the case of the
key as given, so the connector sends `sslMode`, `useCursorFetch`, `connectionTimeZone`,
`forceConnectionTimeZoneToSession` and `rewriteBatchedStatements` in exactly that case.

**Full read** (no `watermark_column`): one load, with `dbtable` set to the quoted table name.

**Watermark read** (`watermark_column` given):

1. **Bound load.** It takes the options of the data load, with two differences: `query` replaces
   `dbtable`, and the user's `partitionColumn`, `lowerBound`, `upperBound` and `numPartitions` are
   left out (compared case-insensitively), because Spark rejects them together with `query`. The
   query is

   ```text
   SELECT CAST(max(<col>) AS CHAR) AS upper_bound, max(<col>) AS upper_bound_value FROM <table>
   ```

   followed by ` WHERE <col> > <value>` when a value is given.
2. **Bound.** The reader reads the `schema` of the bound load's DataFrame once, then calls
   `first()` on it once. The *bound* is the row's first field, by position: the highest watermark
   of the slice as MySQL's text, or `None` when no row qualifies. The *bound's type* is the data
   type of the schema's second field, by position, as Spark maps the column (S3); the reader uses
   it only to choose the bound's literal (decision 34), and reads nothing else of the row.
3. **Data load.** `dbtable` is `(SELECT * FROM <table> WHERE <predicate>) AS ingestio_source`,
   where `<predicate>` is:

   | Value given | Bound | `<predicate>` |
   |---|---|---|
   | yes | not `None` | `<col> > <value> AND <col> <= <bound>` |
   | no | not `None` | `<col> <= <bound>` |
   | either | `None` | `false` |

4. `read()` returns the data load's DataFrame unchanged. The reader adds no Spark-side filter.

`<table>` and `<col>` are quoted names. `<value>` and `<bound>` are literals, built as
[Literals](#literals) says. Every statement uses single spaces, exactly as shown.

What this gives, as for `PostgresReader`:

- The filter runs in MySQL and does not depend on Spark's `pushDownPredicate`.
- Every action on the DataFrame reads the same range of the watermark. Rows written after `read()`
  with a watermark above the bound are left for the next run, so a watermark that the job computes
  from the DataFrame never goes past what the DataFrame holds.
- Rows whose watermark is `NULL` are never read when `watermark_column` is given, first run
  included: `NULL` satisfies neither comparison.
- One index on the watermark column serves both queries.
- The reader imports `pyspark.sql.types` only in a watermark read, to test the bound's type.

### Literals

Every literal the reader writes means the same in every `sql_mode` (M3): a number is a numeric
literal, and any other value is a hex string literal with the `utf8mb4` introducer (decision 33).

**`H(t)`**, for a text `t`, is `_utf8mb4 X'` followed by `t.encode("utf-8").hex()` (lowercase
hexadecimal digits) and `'`. For example `H("O'Brien")` is `_utf8mb4 X'4f27427269656e'`, and
`H("")` is `_utf8mb4 X''`. The text is never escaped otherwise: no quote or backslash of it reaches
the SQL.

**Watermark values.** `watermark_value` is "given" when it is not `None`; `0` and `""` are values.
The literal of a value is built when the reader is built:

| Type | Literal |
|---|---|
| `int`, but not `bool` | `str(value)`, unquoted: `41`, `-7` |
| `decimal.Decimal`, finite | `format(value, "f")`, unquoted: `12.50`, `1000` |
| `str` | `H(value)` |
| `datetime.datetime`, timezone-aware | `H(value.astimezone(UTC).replace(tzinfo=None).isoformat(sep=" "))`: `H("2026-10-01 11:30:00.123456")` |
| `datetime.date` | `H(value.isoformat())` |

- A `str` that UTF-8 cannot encode, such as a lone surrogate, raises the `UnicodeEncodeError` of
  encoding it when the reader is built.
- **Which value for which column.** The value has the column's kind: an `int` for an integer
  column, a `Decimal` for a `DECIMAL` column, a `str` for a character column, a `date` for a
  `DATE` column, an aware `datetime` for a `DATETIME` or `TIMESTAMP` column. MySQL converts a
  string constant compared with a temporal column to a temporal value (K2, to be confirmed). A
  `str` compared with a numeric column, or a number with a character column, is compared as
  floating-point numbers (K1, to be confirmed), which is not exact for large values and orders
  text as numbers.
- **Datetimes.** An aware datetime is converted to UTC and written without its offset, because
  every session of the reader runs in UTC (decision 35): for a `TIMESTAMP` column the literal is the same
  instant as the value, whatever offset the value carried; a `DATETIME` column, which MySQL never
  converts (M4), is compared on the UTC wall clock, as `PostgresReader` compares a
  `timestamp without time zone` column. A `DATETIME` column that holds wall-clock time in another
  zone takes its value as a `str`, which is sent as it is.

**The bound** (decision 34). The bound is MySQL's own text of the highest watermark, never converted. Its
literal is:

- the text as it is, unquoted, when the bound's type is a numeric type of Spark (an instance of
  `pyspark.sql.types.NumericType`) **and** the text, as a whole, matches the regular expression
  `[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?`;
- `H(<text>)` otherwise.

So an integer or decimal column is compared with a numeric literal, exactly (K1), and every other
column with a string constant that MySQL converts to the column's type (K2). A numeric type whose
text is not a number, such as a `BIT` column that Spark 3.5 maps to `LongType` (S3), gets `H` too,
so no text that MySQL returns reaches the SQL unquoted unless it is a number.

The reader rejects, when it is built:

| Case | `ValueError` message |
|---|---|
| `watermark_value` given without `watermark_column` | `watermark_value needs watermark_column` |
| `watermark_column == ""` | `watermark_column must not be empty` |
| `bool`, `float`, or a type not in the table above | `unsupported watermark_value type: <type name>` |
| a `Decimal` that is NaN or infinite | `watermark_value must be finite` |
| a naive `datetime` (its `tzinfo` or its `utcoffset()` is `None`) | `watermark_value must be timezone-aware` |

`<type name>` is `type(value).__name__`. The checks run in the order of the table, after the check
of `table_name` and before the check of `options`, as on `PostgresReader`.

### Time zones

(Decision 35.) Every JDBC session of the reader and of the writer carries `connectionTimeZone=UTC` and
`forceConnectionTimeZoneToSession=true` (J4): Connector/J sets the session's `time_zone` to UTC
and converts `TIMESTAMP` values between the JVM and that zone, which it then knows. What this
gives:

- the bound query and the data load run in sessions with the same time zone, UTC, which has no
  repeated hour, so the bound of a `TIMESTAMP` column round-trips (M4);
- an aware datetime value, normalised to UTC, compares with a `TIMESTAMP` column as the same
  instant;
- a `TIMESTAMP` value read by the reader and written back by the writer, as `SCD2` does when it
  rewrites a dimension, keeps its instant whatever the server's default `time_zone`, where with
  Connector/J's defaults (`LOCAL`, not forced) the values cross as if the server's session ran in
  the JVM's zone (J4).

The driver-side PyMySQL sessions of the writer keep the server's default `time_zone`: the
`INSERT … SELECT` copies values between tables of the same types, which MySQL does not convert,
and the target's defaults and triggers, such as `DEFAULT CURRENT_TIMESTAMP` on a column the
DataFrame lacks, run in that session, as they would for any client.

A user option that replaces `connectionTimeZone` or `forceConnectionTimeZoneToSession` gives these
guarantees up. How Connector/J converts `DATETIME` values under these properties when the JVM's
zone is not UTC is not established (see [Known limitations](#known-limitations)).

### Options

- **Reserved keys** (decision 32). The keys `url`, `dbtable`, `query`, `user`, `password` and `driver`, set
  by the connector, and `sslMode`, `useSSL`, `requireSSL` and `verifyServerCertificate`, which
  would give the executors another TLS mode than the driver's (J1), are reserved. Giving any of
  them in `options`, in any case, raises `ValueError("option '<key>' is set by the connector")`
  when the connector is built, with `<key>` as given.
- Every other key passes through, the connector's defaults included: `useCursorFetch`,
  `fetchsize`, `rewriteBatchedStatements`, `connectionTimeZone`,
  `forceConnectionTimeZoneToSession`, and Connector/J's trust-store properties, such as
  `trustCertificateKeyStoreUrl` (J1).
- Values are passed to Spark as given. Spark turns them into strings, and booleans into `true`
  and `false`.

### Writing

**Operations.**

- `write(data)`, that is with `replace=False`, appends: it inserts every row of `data` into the
  target and leaves the existing rows untouched. `AppendOnly` calls it.
- `write(data, replace=True)` deletes every row of the target and then inserts every row of
  `data`, in the same transaction (decision 3). The target ends up holding exactly the rows of
  `data`. `FullLoad` and `SCD2` call it.
- The calling callback chooses, at call time. The connector's configuration has no mode.

**Sequence.** `<cols>` stands for `data.columns` quoted and joined with `, `. `<target>` is the
quoted target, and `<stg>` the quoted staging table. Every statement is executed through a cursor
of a PyMySQL connection, with `cursor.execute(<statement>)` and no arguments, so that PyMySQL
formats nothing (P6).

0. When `data.columns` is empty, the writer raises `ValueError("the DataFrame has no columns")`
   and does nothing else.
1. **Setup.** The writer opens a connection with the connect arguments, executes
   `CREATE TABLE <stg> LIKE <target>` (decision 36), calls `commit()`, and closes the connection. The
   `CREATE` commits on its own (M1); the `commit()` changes nothing.
2. **Load.** `data.write.format("jdbc").options(...).mode("append").save()`, with the base
   options, `dbtable` = `<stg>`, the write default `rewriteBatchedStatements` = `true` (decision 31, J3),
   the time-zone options (decision 35), and the user's `options`, which replace the connector's options of
   the same name, compared case-insensitively. The executors write in parallel, one transaction
   per partition, into the staging table only.
3. **Apply.** The writer opens a new connection and calls `begin()`. With `replace=True` only, it
   executes `DELETE FROM <target>`. Then it executes
   `INSERT INTO <target> (<cols>) SELECT <cols> FROM <stg>`, and calls `commit()`: the `DELETE`
   and the `INSERT` commit together, or not at all.
4. **Drop.** On the same connection, after the commit, it executes `DROP TABLE <stg>`, calls
   `commit()`, and closes the connection. The `DROP` runs outside the transaction (decision 3); it
   would commit implicitly anyway (M1).

**Staging table.**

- **Creation** (decision 36): `CREATE TABLE <stg> LIKE <target>` copies the target's column definitions,
  indexes and generated columns, and no foreign key (M5). So the staging table has the target's
  primary key, which servers that require one need (K3, to be confirmed); Spark's rows land with
  the types, defaults and checks the target has; and a row that breaks a unique key of the target
  among the rows of `data` fails during the load, before the apply. The foreign keys, the
  triggers and the rows already in the target apply only in step 3. Spark inserts only the
  DataFrame's columns, so the staging table's other columns take their defaults, which the apply
  never reads: the target fills its own. A DataFrame column that the target lacks fails the load
  in Spark, which matches the DataFrame's columns with the staging table's, before any row is
  written.
- **Name** (decision 37): `ingestio_staging_` followed by `uuid.uuid4().hex`, 32 lowercase hexadecimal
  digits, 49 characters in all, within MySQL's 64 (K13). Each call gets a new name. It is created
  in the target's database, `` `<database>`.`ingestio_staging_<hex>` ``, or unqualified, in the
  connection's database, when the target is unqualified.
- **No PyMySQL connection is open while Spark writes**, as for `PostgresWriter`: long writes would
  otherwise run into idle timeouts and firewalls that drop idle connections.

**Atomicity.** Steps 1 and 2 never touch the target. The `DELETE` and the `INSERT` of step 3
commit in one transaction, so other sessions see none of the new rows until the commit, and then
all of them; with `replace=True`, their consistent reads keep seeing the old rows until the commit,
while the `DELETE` holds the row locks of the target (K5, to be confirmed). After a failure at any
step before the commit, the target is as it was. The guarantee needs a transactional storage engine
for the target, such as InnoDB (decision 39).

**Failures.**

| Fails | What the writer does | Then |
|---|---|---|
| step 0 | nothing | `ValueError` |
| step 1 (for example: the target is missing, no `CREATE` privilege) | the connection is closed; there is nothing to clean up | the error propagates |
| step 2, or step 3 up to and including its `commit()` | step 3's connection, if open, is closed without committing; a new connection executes `DROP TABLE IF EXISTS <stg>`, calls `commit()`, and is closed | the original exception propagates, the same object |
| the cleanup, after a failed step 2 or 3 | the original exception gets one note, added with `add_note`: `staging table <stg> could not be dropped: <type>: <message>` | the original exception propagates |
| step 4, after a committed step 3 (decision 38) | the connection is closed; no cleanup runs | the `DROP`'s exception propagates, the same object, with one note: `the rows were committed to <target>, but staging table <stg> could not be dropped` |

- In the notes, `<type>` and `<message>` are the cleanup error's `type(...).__name__` and
  `str(...)`, and `<target>` and `<stg>` are the quoted names. The cleanup error itself is not
  raised.
- A connection is closed with `close()` in every case; MySQL rolls back the open transaction of a
  closed session. The writer never relies on the context manager of a PyMySQL connection.
- If the driver process dies, the cleanup cannot run, and a table named `ingestio_staging_<hex>`
  is left behind.
- The connectors write no log lines.

**What the writer does not do.**

- It runs no DDL on the target. A missing target fails step 1.
- It reads only `data.columns` and `data.write` from the DataFrame, and calls `save()` once. The
  DataFrame is evaluated exactly once, by the save.
- It never counts or inspects rows. An empty DataFrame goes through the same sequence: an append
  inserts nothing, and `replace=True` leaves the target empty.
- `write` returns `None`.

**PyMySQL.** The writer calls `pymysql.connect` as an attribute of the `pymysql` module each time
it opens a connection, with the connect arguments as keyword arguments and no positional argument.
Building a `MySQLWriter` imports `pymysql`. When that import fails, the writer raises
`ImportError("MySQLWriter needs PyMySQL: pip install 'ingestio[mysql]'")`, chained to the original
error. The reader never imports PyMySQL.

### The `mysql` extra

Following the pattern of [specs/postgres_connectors.md](postgres_connectors.md), decision 19:

- `[project.optional-dependencies]` gains `mysql = ["PyMySQL[rsa]>=1.2.0"]` (decision 25): PyMySQL 1.2.0 or
  later, whose default without an ssl argument is `PREFERRED` (P3), with its `rsa` extra, which
  installs `cryptography` for `caching_sha2_password` over a connection without TLS (P5).
  `python-mysql-replication` is not in it (decision 42).
- Only `MySQLWriter` needs the extra. `MySQLConnection` and `MySQLReader` import no PyMySQL.
- pyspark and MySQL Connector/J are prerequisites, never in an extra.
- The `dev` dependency group gains `PyMySQL[rsa]>=1.2.0`, and `types-PyMySQL>=1.2`, because PyMySQL
  ships no `py.typed` marker and mypy strict checks `src/` and `tests/` (P7).

### In a configuration

The `mysql` system (decision 26) is two more rows of the closed table:

| `system` | `source` builds | `target` builds | Extra the connector may need |
|---|---|---|---|
| `mysql` | `MySQLReader` | `MySQLWriter` | `mysql`, for the writer |

| `system` | Section | Required keys | Optional keys |
|---|---|---|---|
| `mysql` | source | `table_name` | `watermark_column`, `options` |
| `mysql` | target | `table_name` | `options` |

| `system` | Section | `connection` | `client` | `watermark_value` | `spark` |
|---|---|---|---|---|---|
| `mysql` | source | **required**: a `MySQLConnection` | no | optional | optional |
| `mysql` | target | **required**: a `MySQLConnection` | no | – | – |

- **Keys.** Each key is the constructor argument of the same name, passed as it is, with the JSON
  types the other systems use for them: `table_name` and `watermark_column` strings, `options` an
  object of strings, numbers and booleans. The host, the database, the user, the password, the port
  and `ssl_mode` are never keys: like any key the system does not take, they raise
  `<path>: <section>.<key> is not a key of system 'mysql'`.
- **Lists of systems.** `<names>` for a source becomes
  `bigquery, disk, gcs, json_api, mongodb, mysql, mysql_cdc, postgres, s3, unity_catalog, volume`,
  and for a target `bigquery, disk, gcs, json_lines, mysql, postgres, s3, unity_catalog, volume`.
- **`connection`.** The callback hands the very object to the connector. When it is missing, the
  build raises `<path>: <section> system 'mysql' needs the argument connection`.
- **The type of `connection`** ([specs/mongodb_reader.md](mongodb_reader.md), decision 10). The
  callback checks that it is an instance of `MySQLConnection`, a subclass included, after the other
  checks of the callback arguments and before it builds the connector; otherwise it raises
  `ValueError("<path>: <section> system 'mysql' needs a MySQLConnection as the argument connection, not <type name>")`.
  The checks of `postgres` and `mongodb` are unchanged: a `MySQLConnection` given to them raises
  their message, `... needs a PostgresConnection as the argument connection, not MySQLConnection`.
- **Errors of the connector.** A `ValueError` or `ImportError` that the connector raises when the
  callback builds it propagates as the same object, with the note
  `while building the <section> of <path>, system 'mysql'`, as for every system.
- Building a callback builds the connector and nothing else: no Spark session, no network, no
  PyMySQL connection.

### SCD2

(Decision 40.) `mysql` joins the targets that `SCD2` supports, which become `bigquery`, `mysql`, `postgres`
and `unity_catalog`. Decision 26 of [specs/scd2_callback.md](scd2_callback.md) admits the systems
whose `write(data, replace=True)` is atomic and stays correct when the DataFrame written is a lazy
plan that reads the same target. For `mysql`:

| Full read of the target | `write(data, replace=True)` | Atomic | `data` reads the target |
|---|---|---|---|
| one JDBC statement over the table, in one partition | Spark loads a staging table from `data`; then one transaction runs `DELETE` and `INSERT … SELECT`; the staging table is dropped after the commit | yes: one transaction; before the commit, a failure leaves the target unchanged and the staging table is dropped | `data` is evaluated whole, into the staging table, before the transaction touches the target |

- The target's reader is `MySQLReader`, built when the process runs with the `target` section's
  `table_name`, the `connection` that the writer received, and `spark=state[key].sparkSession`, as
  decision 25 of that spec describes; the section's `options` go to the writer only.
- The narrowed list of the message becomes
  `<path>: target.system '<name>' is not one of: bigquery, mysql, postgres, unity_catalog`.
- Each run rewrites the whole dimension, which on `mysql` means a `DELETE` of every row and an
  `INSERT` of every row in one transaction, with the cost of decision 3.

### Where it lives

- `src/ingestio/connector/mysql.py` (decision 26): `MySQLConnection`, `MySQLReader` and `MySQLWriter`, with
  a module docstring and a sorted `__all__`. pyspark and PyMySQL are imported lazily, and for
  annotations under `TYPE_CHECKING`.
- `src/ingestio/connector/all.py`: exports the three classes.
- `src/ingestio/callback/_configuration.py`: the `mysql` rows of the source and target tables,
  with `MySQLConnection` as their connection type, and `mysql` among `SCD2`'s systems (decision 40).
- `src/ingestio/callback/io.py`: the annotations and docstrings of `connection` in `ReadSource`,
  `AppendOnly`, `FullLoad` and `SCD2`.
- `pyproject.toml` and `uv.lock`, outside every stage's write scope: the extra
  `mysql = ["PyMySQL[rsa]>=1.2.0"]`, and `PyMySQL[rsa]>=1.2.0` and `types-PyMySQL>=1.2` in the
  `dev` group (see [The `mysql` extra](#the-mysql-extra); P7). The main session makes these changes
  after approval, as it did for the PostgreSQL connectors.
- No change to `ingestio/__init__.py`, `ingestio/basics.py`, `connector/core.py` or the other
  connector modules.

### What becomes outdated

The documentation stage updates `docs/` and `.claude/kb/`; the README is the maintainer's.

- `docs/concepts/connectors.md`: a MySQL section (connection, reader, writer, literals, time
  zones, environments, limitations) and the Extras table.
- `docs/concepts/configuration.md`: the tables of systems, keys and callback arguments; the
  description of `connection`; the lists of `<names>`; the type-check message; `SCD2`'s list.
  Its example at the end of [Example](../docs/concepts/configuration.md#example) uses
  `{"target": {"system": "mysql"}}` as a system that does not exist: it needs another name.
- `docs/concepts/callbacks.md`: `connection` of the four callbacks and `SCD2`'s supported targets.
- ADR 0012, ADR 0014 and ADR 0015: see [ADRs](#adrs). `docs/index.md`: this spec, and the
  description of the connectors page.
- `.claude/kb/architecture/quick-reference.md`: modules, configuration, public API, known gaps.
- `docs/backlog.md`, idea 8: its entry already links to this spec; the main session updates it.
- Tests of other specs that list the systems, or that use `mysql` as the name of a system that does
  not exist; the test-writer of this spec updates them, with no change to what they check:
  - `tests/test_config_connectors.py`: `SOURCE_NAMES`, and the case `("source", "mysql")` of
    `test_at012_...`;
  - `tests/test_append_only_callback.py`: `TARGET_NAMES`, and the name `mysql` among the unknown
    systems of the test around line 1215;
  - `tests/test_mongodb_reader.py`: `SOURCE_NAMES` and `TARGET_NAMES`;
  - `tests/test_scd2_callback.py`: `SUPPORTED`, the entry `"mysql"` of `UNSUPPORTED` and the case
    `{"target": {"system": "mysql"}}` of `test_at008_...` (decision 40).
- Validated specs whose criteria use `mysql` as the name of a system that does not exist:
  [config_connectors](config_connectors.md) (the table of unknown names, around line 696),
  [append_only_callback](append_only_callback.md) (around line 593) and
  [scd2_callback](scd2_callback.md) (AT-005 and AT-008). The main session amends them, as it
  amended AT-004 of the PostgreSQL spec, by replacing the name.
- `README.md`: the connectors table gains a `mysql` row, for the maintainer.

### Supported environments

- **pyspark** 3.5 or later, a prerequisite.
- **MySQL Connector/J** on the session's classpath, a prerequisite JAR, from 8.0.23 (decision 41), the
  first release with the time-zone properties of J4; `sslMode` exists since 8.0.13 (J1). For
  example `spark.jars.packages=com.mysql:mysql-connector-j:<version>` (K9). Whether a Databricks
  Runtime ships Connector/J is not established: a job installs it as a cluster library.
- **Databricks classic compute** on Databricks Runtime 15.4 LTS or later, in dedicated access
  mode, with Connector/J as a cluster library. Standard access mode, which restricts JAR
  libraries, and serverless compute: no claim. Only the DataFrame API is used.
- **Open-source Spark** 3.5 or later, on a Java release that Spark version supports, with
  Connector/J on the classpath.
- **MySQL** (decision 41): 8.4 LTS and 9.x, the lines M6 names as supported. MySQL 8.0 and earlier, and
  MariaDB, whose JDBC URL `jdbc:mariadb:` Spark's MySQL dialect does not match (S1): no claim.
- **PyMySQL** 1.2.0 or later, on the driver, for the writer (decision 25).
- **Network.** The executors reach MySQL through Connector/J; for the writer, the driver reaches it
  through PyMySQL too.
- **Privileges**, as far as the planner knows: the reader needs `SELECT` on the table. The writer
  needs `CREATE`, `DROP`, `INSERT` and `SELECT` on the target's database, for the staging table
  (K6, to be confirmed), `SELECT` and `INSERT` on the target, and `DELETE` for `replace=True`.

### Known limitations

These are documented, not solved here.

- **Late commits.** A row committed after a read, with a watermark at or below the bound that read
  captured, is never read by a later run, as for `PostgresReader`. Rows that share the exact value
  of the previous watermark but commit later are missed the same way.
- **Full reads are not frozen.** Every action on a full read's DataFrame queries the table again.
- **`DELETE` on large tables** (decision 3): `write(data, replace=True)` deletes every row in the
  transaction, which is slow on a large table and grows the undo log. While it runs it holds the
  row locks of the target, so other writers wait; consistent reads see the old rows until the
  commit (K5).
- **Foreign keys and triggers on a replace** (K5, to be confirmed). The target keeps its incoming
  foreign keys and its triggers (decision 3), and the `DELETE` fires them: a referencing table
  whose foreign key is `ON DELETE CASCADE` loses its child rows, one with `ON DELETE SET NULL` gets
  `NULL`s, and one with `RESTRICT` or `NO ACTION` makes the replace fail, with the target
  unchanged, even when `data` holds the same keys. The target's `DELETE` triggers fire once per
  row, and its `INSERT` triggers once per inserted row.
- **The binary log** (K11, to be confirmed). In `ROW` format, a replace writes a delete event for
  every row of the target and an insert event for every row of `data`, and the staging table's
  rows go through the binary log and reach the replicas too. A CDC reader of the target, such as
  the later spec's, would see all of them.
- **Non-transactional engines** (decision 39). A target in an engine without transactions, such as MyISAM,
  gets no atomicity: a failure in step 3 can leave part of the rows, or the target emptied. The
  writer does not check the engine.
- **One long transaction.** The apply copies the whole batch in one transaction, which is bounded
  by the server's limits, such as `max_binlog_cache_size` and, under Group Replication,
  `group_replication_transaction_size_limit` (K10, to be confirmed).
- **Spark task retries.** Spark's JDBC writer is not idempotent per partition. A task re-run after
  its partition committed writes the partition twice into the staging table: the copy reaches the
  target, or, when the target has a unique key that the staging table copied, the load fails with
  a duplicate key.
- **Server-side cursors** (decision 30). With `useCursorFetch`, MySQL materialises each partition's result
  on the server before the first row is fetched (K4, to be confirmed).
- **Type conversion.** The values of the rows go through Spark's JDBC type mapping, which depends
  on the Spark version (S3), and through Connector/J. The connectors convert no value of their
  own, apart from the watermark value's normalisation to UTC; the time-zone options (decision 35) set the
  session's zone. How Connector/J converts `DATETIME` values when the JVM's zone is not UTC is not
  established.
- **Backticks in column names on Spark 3.5.** Spark quotes the DataFrame's column names in its own
  SQL without doubling a backtick (S2), so a column whose name holds one fails the read or the
  load on Spark 3.5.0.
- **Values of the wrong kind** (K1). A watermark value whose kind differs from the column's, such
  as a `str` for a `BIGINT` column, compares as floating-point numbers.
- **Collations.** A `str` watermark compares in MySQL's collation rules; only `utf8mb4` columns are
  covered by the integration tests.
- **The staging table is visible** to other sessions of the database while it lives, under its
  `ingestio_staging_` name, and holds a copy of the batch.
- **Orphaned staging tables**, when the driver dies, when the cleanup fails, or when the `DROP`
  after the commit fails (decision 38).
- **Not verified.** The facts K1 to K16 that remain are the planner's knowledge, not established by a source
  read for this spec. The integration tests below check the ones they can; the others stay claims
  to confirm.

### Notes for testing

Every criterion has at least one test that always runs, with test doubles, without a JVM, a
network or a MySQL server. The doubles live in `tests/test_mysql_connectors.py` and share one
ordered event log. No criterion needs Spark; only the integration tests do.

- **Fake Spark session**, as in `tests/test_postgres_connectors.py`. Each access to `spark.read`
  returns a new fake reader, which supports `format(name)`, `option(key, value)` and
  `options(**kwargs)`, and records one *load* each time `load()` is called, with the format and the
  merged options: a later call replaces an earlier key with the same name, compared
  case-insensitively, as Spark does; the original case of every key is kept. Values are compared as
  Spark sees them: `str(value)`, and booleans as `true` and `false`.
- **Fake DataFrame.** `load()` returns a new fake DataFrame and records every attribute accessed on
  it. A DataFrame from a load with a `query` option has a `schema`, a real
  `pyspark.sql.types.StructType` that the test chooses, by default
  `StructType([StructField("upper_bound", StringType()), StructField("upper_bound_value", TimestampType())])`,
  and answers `first()` with the row the test chose, a two-element tuple such as
  `("2026-10-21 09:30:00.500000", None)` or `(None, None)`; accessing `schema` and calling
  `first()` are events. Importing `pyspark.sql.types` starts no JVM. A DataFrame given to the writer
  has `columns` and `write`; `write` returns a fake writer that supports `format`, `option`,
  `options` and `mode`, and records a *save*, with its format, mode and merged options.
- **Fake PyMySQL.** The test replaces `pymysql.connect` with `monkeypatch.setattr`. Each call
  records its positional and keyword arguments and returns a fake connection that supports
  `cursor()`, `begin()`, `commit()`, `rollback()` and `close()`, each recorded. `cursor()` returns
  a fake cursor, usable as a context manager, whose `execute(query, args=None)` records the
  statement and its arguments on its connection. A statement is *committed* when its connection
  calls `commit()` after executing it, with no `rollback()` in between. Any of these calls can be
  made to raise.
- `pyspark.sql.SparkSession.active` can be replaced with `monkeypatch.setattr`. A missing package
  is simulated in a subprocess, or with `monkeypatch.setitem`, by mapping its name to `None` in
  `sys.modules`.
- **Configurations** are written with `json.dumps` to a file under `tmp_path`, as in
  `tests/test_config_connectors.py`. A *twin* is a connector built directly from
  `ingestio.connector.all` over a second set of fakes in the same initial state.
- Tests use the public API, and pass the fakes as `spark=` and as DataFrames, with `typing.cast`
  to satisfy mypy strict, which checks `tests/` too.

### Integration tests

On top of the criteria, real tests against a MySQL server, marked `@pytest.mark.integration` and
opt-in (decision 43). Each test creates its own uniquely named tables and drops them. They are written
blind where the stage that writes them cannot run them, and run in CI once the maintainer adds a
MySQL server and the variables below.

**Tier 1**, skipped with a reason unless `INGESTIO_MYSQL_URL` is set, of the form
`mysql://<user>:<password>@<host>:<port>/<database>`. No Spark: the writer runs for real, with a
fake DataFrame whose save inserts its rows into the table named by its `dbtable` option through a
PyMySQL connection of the test.

1. `write` appends. Columns the DataFrame lacks take the target's defaults, an `AUTO_INCREMENT`
   column included. No table whose name starts with `ingestio_staging_` is left.
2. `write(df, replace=True)` leaves exactly the DataFrame's rows; with an empty DataFrame that has
   the right columns, it leaves the target empty.
3. A row that conflicts with an existing row on a unique key fails the apply; the target is
   unchanged and no staging table is left.
4. `write(df, replace=True)` on a target referenced with `ON DELETE RESTRICT` fails and leaves it
   unchanged; referenced with `ON DELETE CASCADE`, it deletes the child rows (K5).
5. A save that fails after inserting some rows into the staging table leaves the target unchanged
   and no staging table.
6. Run under a server `sql_mode` with `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES`, where the test may
   set it, the writer's statements still succeed, with a column named ``a"b`c``.

**Tier 2**, skipped with a reason unless also `INGESTIO_SPARK_TESTS` is `1` and
`INGESTIO_MYSQL_JDBC_PACKAGE` holds the Maven coordinate of a Connector/J release, from which a
local Spark session gets the JAR through `spark.jars.packages`; the file runs in its own process,
as the Unity Catalog and BigQuery integration tests do.

1. A full read of a table with `BIGINT`, `DECIMAL(10,2)`, `DATETIME(6)`, `TIMESTAMP(6)`, `VARCHAR`
   in `utf8mb4` and `JSON` columns returns every row.
2. A watermark read on a `BIGINT` column with values above 2^53 returns exactly the rows above the
   value; a row inserted after `read()` with a higher watermark is absent from a second action on
   the same DataFrame; a row with a `NULL` watermark is never returned.
3. Watermark reads on `DATETIME(6)` and `TIMESTAMP(6)` columns with an aware `datetime` value
   return exactly the expected rows while the server's default `time_zone` is `-03:00` (K2).
4. A watermark read on a `VARCHAR` column with a value and a bound that hold `'` and `\` returns
   exactly the expected rows, with and without `NO_BACKSLASH_ESCAPES`, where the test may set it.
5. `TIMESTAMP(6)` values read and written back with `write(df, replace=True)` keep their instants
   while the server's default `time_zone` is `-03:00`.
6. A value that cannot be stored in the target column's type fails during the Spark load; the
   target is unchanged and no staging table is left.
7. A DataFrame column that the target lacks fails before any row is written; the target is
   unchanged and no staging table is left.

## Acceptance criteria

Every criterion follows the maintainer's answers, recorded under [Decisions](#decisions); a
criterion that rests on one of decisions 24 to 43 names it. Unless a criterion says otherwise:

- `conn = MySQLConnection("db.internal", "shop", "ingest", "s3cret")`;
- `spark` is the fake session, and DataFrames and `pymysql.connect` are the doubles of
  [Notes for testing](#notes-for-testing);
- the reader is `MySQLReader("shop.orders", conn, spark=spark)`, and the writer is
  `MySQLWriter("shop.orders", conn)`;
- the **base options** are `url` = `jdbc:mysql://db.internal:3306/shop`, `user` = `ingest`,
  `password` = `s3cret` and `driver` = `com.mysql.cj.jdbc.Driver`;
- the **read defaults** are `useCursorFetch` = `true` and `fetchsize` = `10000` (decision 30), and the
  **time-zone options** are `connectionTimeZone` = `UTC` and
  `forceConnectionTimeZoneToSession` = `true` (decision 35);
- the **connect arguments** are
  `host="db.internal", port=3306, user="ingest", password="s3cret", database="shop", charset="utf8mb4"`;
- `H(t)` is the literal of [Literals](#literals): `_utf8mb4 X'` + `t.encode("utf-8").hex()` + `'`;
- `<stg>` is `` `shop`.`ingestio_staging_<hex>` ``, where `<hex>` is 32 lowercase hexadecimal
  digits, the same throughout one call;
- `df` is a fake DataFrame with `columns == ["id", "payload"]`;
- "exactly the options" means these keys and no others, compared case-insensitively unless the
  criterion says the case matters;
- `path` is a configuration file under `tmp_path`, `<path>` is `os.fspath(path)`, and the default
  sections are `{"system": "mysql", "table_name": "shop.orders"}` as `source` or as `target`;
- every error of a configuration is raised by the call that builds the callback, before any
  `Process` exists.

### Import surface and the extra

- **AT-001**: Given a fresh interpreter, when
  `from ingestio.connector.all import MySQLConnection, MySQLReader, MySQLWriter` runs, then it
  succeeds; the three names are in `ingestio.connector.all.__all__` and in
  `ingestio.connector.mysql.__all__` (decision 26), and none of them is in `ingestio.__all__`.
- **AT-002**: Given an interpreter where `import pyspark` and `import pymysql` both raise
  `ImportError`, when `ingestio.connector.all` is imported, then the import succeeds; a
  `MySQLConnection` with each `ssl_mode` and a `MySQLReader` with `spark=<fake session>` can be
  built, and its full read returns the DataFrame of the fake's load.
- **AT-003**: Given an interpreter where `import pymysql` raises `ImportError`, when
  `MySQLWriter("shop.orders", conn)` is built, then it raises `ImportError` with the message
  `MySQLWriter needs PyMySQL: pip install 'ingestio[mysql]'`, whose `__cause__` is the error of the
  import. In the same interpreter, the reader's full and watermark reads run.
- **AT-004**: Given the metadata of the installed distribution,
  `importlib.metadata.metadata("ingestio")`, then `mysql` is among its `Provides-Extra` values,
  every `Requires-Dist` carries an `extra == "<name>"` marker, and one `Requires-Dist` names the
  project `PyMySQL` (compared after PEP 503 normalisation), with the extra `rsa`, the specifier
  `>=1.2.0` and the marker `extra == "mysql"` (decisions 25 and 42).

### `MySQLConnection`

- **AT-005**: Given the calls below, then each raises `ValueError` with the message shown:

  | Call | Message |
  |---|---|
  | `MySQLConnection("", "shop", "ingest", "s3cret")` | `host must not be empty` |
  | `MySQLConnection("db.internal", "", "ingest", "s3cret")` | `database must not be empty` |
  | `MySQLConnection("db.internal", "shop", "", "s3cret")` | `user must not be empty` |
  | `MySQLConnection(..., port=0)` and `port=65536` | `port must be between 1 and 65535` |
  | `MySQLConnection(..., ssl_mode="required")` | `unsupported ssl_mode 'required'` |
  | `MySQLConnection(..., ssl_mode="VERIFY_FULL")` | `unsupported ssl_mode 'VERIFY_FULL'` |

  `port=1`, `port=65535`, `password=""`, and each of `DISABLED`, `PREFERRED`, `REQUIRED`,
  `VERIFY_CA` and `VERIFY_IDENTITY` as `ssl_mode` are accepted.
- **AT-006**: Then `repr(conn)` and `str(conn)` are
  `MySQLConnection(host='db.internal', port=3306, database='shop', user='ingest')`;
  `repr(MySQLReader("shop.orders", conn))` is
  `MySQLReader(table_name='shop.orders', host='db.internal', database='shop')`; and
  `repr(MySQLWriter("shop.orders", conn))` is
  `MySQLWriter(table_name='shop.orders', host='db.internal', database='shop')`. Given in addition
  `ssl_mode="REQUIRED"`, a watermark and `options`, the `repr` of the reader and the writer is the
  same.
- **AT-007**: Given `conn`, then its attributes `host`, `database`, `user`, `password`, `port` and
  `ssl_mode` are `"db.internal"`, `"shop"`, `"ingest"`, `"s3cret"`, `3306` and `None`, and
  assigning to any of them raises `dataclasses.FrozenInstanceError`.

### Building the connectors

- **AT-008**: Given `name` in `""`, `"orders."`, `".orders"` and `"a.b.c"`, when
  `MySQLReader(name, conn)` or `MySQLWriter(name, conn)` is built, then it raises
  `ValueError("table_name must be 'table' or 'database.table'")` (decision 28). `"orders"` and
  `"shop.orders"` are accepted by both.
- **AT-009**: Given `MySQLReader("shop.orders", conn, watermark_value=41)`, then it raises
  `ValueError("watermark_value needs watermark_column")`. Given `watermark_column=""`, then it
  raises `ValueError("watermark_column must not be empty")`.
- **AT-010**: Given `watermark_column="updated_at"` and the `watermark_value` on the left, when
  the reader is built, then it raises `ValueError` with the message on the right:

  | `watermark_value` | Message |
  |---|---|
  | `True` | `unsupported watermark_value type: bool` |
  | `1.5` | `unsupported watermark_value type: float` |
  | `[1]` | `unsupported watermark_value type: list` |
  | `Decimal("NaN")` | `watermark_value must be finite` |
  | `Decimal("Infinity")` | `watermark_value must be finite` |
  | `datetime(2026, 10, 1)` | `watermark_value must be timezone-aware` |

  `0`, `""`, `Decimal("12.50")`, `date(2026, 10, 1)` and `datetime(2026, 10, 1, tzinfo=UTC)` are
  accepted.
- **AT-011**: Given `key` in `url`, `DbTable`, `QUERY`, `user`, `password`, `Driver`, `sslMode`,
  `SSLMODE`, `useSSL`, `requireSSL` and `VerifyServerCertificate`, when
  `MySQLReader("shop.orders", conn, options={key: "x"})` or
  `MySQLWriter("shop.orders", conn, options={key: "x"})` is built, then it raises `ValueError`
  with the message `option '<key>' is set by the connector`, with the key as given (decision 32). Both
  accept `{"useCursorFetch": False, "fetchsize": 500, "rewriteBatchedStatements": False, "connectionTimeZone": "SERVER", "trustCertificateKeyStoreUrl": "file:/certs/ca.jks", "sessionInitStatement": "SET SESSION sql_mode = 'ANSI'"}`.
- **AT-012**: Given `pymysql.connect` replaced and `pyspark.sql.SparkSession.active` replaced with
  a function that raises, when readers, with and without a watermark and `spark`, and writers, with
  and without `options`, are built, then each succeeds, no load is recorded, no attribute of
  `spark` is accessed, `active` is not called and `pymysql.connect` is not called.

### Full read

- **AT-013**: When `reader.read()` runs, then exactly one load is recorded, with the format `jdbc`
  and exactly the options: the base options, `dbtable` = `` `shop`.`orders` ``, the read defaults
  and the time-zone options, with the keys `useCursorFetch`, `fetchsize`, `connectionTimeZone` and
  `forceConnectionTimeZoneToSession` in this case. `read()` returns the very DataFrame that `load()`
  returned, and no attribute of it was accessed. The `url` option contains neither `s3cret` nor
  `@`.
- **AT-014**: Given `MySQLConnection("::1", "my db", "ingest", "s3cret", port=3307)`, when a
  reader with it reads, then the `url` option is `jdbc:mysql://[::1]:3307/my%20db`.
- **AT-015**: Given `options={"partitionColumn": "id", "lowerBound": 1, "upperBound": 1000,
  "numPartitions": 8, "FetchSize": 500, "UseCursorFetch": False, "connectionTimeZone": "SERVER"}`,
  when the reader reads, then the load carries `partitionColumn` = `id`, `lowerBound` = `1`,
  `upperBound` = `1000`, `numPartitions` = `8`, and a single option for each of the fetch size, the
  cursor fetch and the connection time zone, the user's, with the values `500`, `false` and
  `SERVER` and the keys as the user wrote them. The base options, `dbtable` and
  `forceConnectionTimeZoneToSession` are as in AT-013.
- **AT-016**: Given `ssl_mode="VERIFY_IDENTITY"` in the connection, when the reader reads, with or
  without a watermark, then every load carries `sslMode` = `VERIFY_IDENTITY`, with this exact key.
  Without `ssl_mode`, no load carries an option named `sslMode` in any case.
- **AT-017**: Given a reader built without `spark`, while `pyspark.sql.SparkSession.active` is
  replaced with a function that raises, then building succeeds. When `active` is then replaced with
  a function that returns the fake session and `read()` runs, then the load is recorded on the fake.
  Given `active` replaced with a function that raises `err` when `read()` runs, then `read()`
  raises `err` itself.

### Watermark read

The reader in this section is
`MySQLReader("shop.orders", conn, spark=spark, watermark_column="updated_at", ...)`. The bound load's
DataFrame has the default schema of the doubles and answers `("2026-10-21 09:30:00.500000", None)`
unless a criterion says otherwise.

- **AT-018**: Given `watermark_value=datetime(2026, 10, 1, tzinfo=UTC)`, when `read()` runs, then
  the event log shows, in this order:
  1. a load with the format `jdbc` and exactly the base options, the read defaults, the time-zone
     options and `query` =

     ```text
     SELECT CAST(max(`updated_at`) AS CHAR) AS upper_bound, max(`updated_at`) AS upper_bound_value FROM `shop`.`orders` WHERE `updated_at` > H('2026-10-01 00:00:00')
     ```

  2. an access to `schema` on that load's DataFrame;
  3. one `first()` on it;
  4. a load with the format `jdbc` and exactly the base options, the read defaults, the time-zone
     options and `dbtable` =

     ```text
     (SELECT * FROM `shop`.`orders` WHERE `updated_at` > H('2026-10-01 00:00:00') AND `updated_at` <= H('2026-10-21 09:30:00.500000')) AS ingestio_source
     ```

  where each `H(...)` stands for its literal, so the first is
  `_utf8mb4 X'323032362d31302d30312030303a30303a3030'`. `read()` returns the second load's
  DataFrame, and no attribute of it was accessed. No other load is recorded.
- **AT-019**: Given no `watermark_value`, when `read()` runs, then the first load's `query` is
  ``SELECT CAST(max(`updated_at`) AS CHAR) AS upper_bound, max(`updated_at`) AS upper_bound_value FROM `shop`.`orders` ``
  with nothing after the table, and the second load's `dbtable` is
  ``(SELECT * FROM `shop`.`orders` WHERE `updated_at` <= H('2026-10-21 09:30:00.500000')) AS ingestio_source``.
- **AT-020**: Given the bound load answers `(None, None)`, with or without a `watermark_value`,
  when `read()` runs, then the second load's `dbtable` is
  ``(SELECT * FROM `shop`.`orders` WHERE false) AS ingestio_source``, and `read()` returns that
  load's DataFrame.
- **AT-021**: Given the `watermark_value` on the left, when `read()` runs, then both loads contain
  `` `updated_at` > `` followed by the literal on the right (decision 33):

  | `watermark_value` | Literal |
  |---|---|
  | `41` | `41` |
  | `0` | `0` |
  | `-7` | `-7` |
  | `Decimal("12.50")` | `12.50` |
  | `Decimal("1E+3")` | `1000` |
  | `Decimal("-0.25")` | `-0.25` |
  | `"41"` | `_utf8mb4 X'3431'` |
  | `"O'Brien"` | `_utf8mb4 X'4f27427269656e'` |
  | `r"C:\temp"` | `_utf8mb4 X'433a5c74656d70'` |
  | `""` | `_utf8mb4 X''` |
  | `"é"` | `_utf8mb4 X'c3a9'` |
  | `date(2026, 10, 1)` | `_utf8mb4 X'323032362d31302d3031'` |
  | `datetime(2026, 10, 1, 8, 30, tzinfo=UTC)` | `H('2026-10-01 08:30:00')` |
  | `datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-3)))` | `H('2026-10-01 11:30:00.123456')` |

  No load contains a single quote other than those of the `X'…'` literals.
- **AT-022**: Given `watermark_column="w"`, no `watermark_value`, and the bound load's schema whose
  second field has the type on the left and whose `first()` answers the text in the middle, when
  `read()` runs, then the second load's `dbtable` contains `` `w` <= `` followed by the literal on
  the right (decision 34):

  | Type of `upper_bound_value` | Bound text | Literal |
  |---|---|---|
  | `LongType()` | `9007199254740993` | `9007199254740993` |
  | `IntegerType()` | `-7` | `-7` |
  | `DecimalType(18, 2)` | `12.50` | `12.50` |
  | `DecimalType(20, 0)` | `18446744073709551615` | `18446744073709551615` |
  | `DoubleType()` | `1e20` | `1e20` |
  | `LongType()` | `1 OR 1=1` | `H('1 OR 1=1')` |
  | `StringType()` | `41` | `_utf8mb4 X'3431'` |
  | `StringType()` | `it's` | `H("it's")` |
  | `TimestampType()` | `2026-10-21 09:30:00` | `H('2026-10-21 09:30:00')` |
  | `DateType()` | `2026-10-21` | `H('2026-10-21')` |
  | `BooleanType()` | `1` | `H('1')` |

- **AT-023**: Given the `table_name` and `watermark_column` on the left, when a full read runs, its
  `dbtable` is the quoted table. When a watermark read with `watermark_value=1` runs, both queries
  use the quoted table and the quoted column:

  | `table_name` | `watermark_column` | Quoted table | Quoted column |
  |---|---|---|---|
  | `orders` | `updated_at` | `` `orders` `` | `` `updated_at` `` |
  | `Sales.Orders` | `UpdatedAt` | `` `Sales`.`Orders` `` | `` `UpdatedAt` `` |
  | ``shop.my`table`` | ``my`col`` | ``` `shop`.`my``table` ``` | ``` `my``col` ``` |
  | `shop.a"b` | `c"d` | `` `shop`.`a"b` `` | `` `c"d` `` |

- **AT-024**: Given `watermark_value=1` and `options={"partitionColumn": "id", "lowerBound": 1,
  "upperBound": 1000, "NumPartitions": 8, "queryTimeout": 30}`, when `read()` runs, then the bound
  load carries `queryTimeout` = `30` and none of `partitionColumn`, `lowerBound`, `upperBound` or
  `numPartitions`, in any case. The data load carries all five.
- **AT-025**: Given a fake whose `load()` raises `err = RuntimeError("down")`, when `read()` runs,
  with or without a watermark, then it raises `err` itself, and no load is recorded after the one
  that raised. Given a bound DataFrame whose `first()` raises `err`, then `read()` raises `err`
  itself and records no second load.
- **AT-026**: Given a reader with a watermark, when `read()` runs twice and the bound load answers
  `("2026-10-21 09:30:00.500000", None)` the first time and `("2026-10-22 10:00:00", None)` the
  second, then four loads are recorded, each read's data load uses its own bound, and the first
  `read()`'s DataFrame is not the second's.

### Writing

- **AT-027**: When `writer.write(df)` runs, then the event log shows, in this order:
  1. a call of `pymysql.connect` with exactly the connect arguments, as keyword arguments, and no
     positional argument; on that connection, the execution of
     ``CREATE TABLE <stg> LIKE `shop`.`orders` `` (decision 36), then `commit()`, then `close()`;
  2. one save, with the format `jdbc`, the mode `append`, and exactly the options: the base
     options, `dbtable` = `<stg>`, `rewriteBatchedStatements` = `true` (decision 31) and the time-zone
     options;
  3. a second call of `pymysql.connect` with the same arguments; on that connection, `begin()`, the
     execution of
     ``INSERT INTO `shop`.`orders` (`id`, `payload`) SELECT `id`, `payload` FROM <stg>``, then
     `commit()`, the execution of `DROP TABLE <stg>`, `commit()`, and `close()`.

  Every statement is executed with no argument besides it. No other connection is opened, no other
  statement is executed, no `rollback()` is called, no load or other save is recorded, and `write`
  returns `None`. `writer.write(df, replace=False)` gives the same event log.
- **AT-028**: When `writer.write(df, replace=True)` runs, then the event log is the one of AT-027,
  except that on the second connection the execution of ``DELETE FROM `shop`.`orders` `` comes after
  `begin()` and before the `INSERT`, and the first `commit()` commits both.
- **AT-029**: Given two consecutive `write(df)` calls, then each uses a staging name that matches
  `ingestio_staging_[0-9a-f]{32}`, and the two names differ (decision 37). Given
  `MySQLWriter("orders", conn)`, then the staging table is the unqualified
  `` `ingestio_staging_<hex>` `` in the `CREATE`, the save's `dbtable`, the `INSERT` and the `DROP`,
  and the target is `` `orders` ``.
- **AT-030**: Given `options={"numPartitions": 4, "BatchSize": 5000, "RewriteBatchedStatements": False}`,
  when `write(df)` runs, then the save carries `numPartitions` = `4`, `BatchSize` = `5000`, and a
  single option for the batched rewrite, the user's, `RewriteBatchedStatements` = `false`. The
  other options are as in AT-027. Without user options, the save's keys `rewriteBatchedStatements`,
  `connectionTimeZone` and `forceConnectionTimeZoneToSession` are in exactly this case. In every
  `write` of this section, with or without `replace=True`, the only attributes accessed on the
  DataFrame are `columns` and `write`, and `save()` is called once.
- **AT-031**: Given the `ssl_mode` on the left in the connection, when `write(df)` runs, then every
  `pymysql.connect` call receives exactly the connect arguments plus the arguments on the right, as
  keyword arguments, and the save carries the `sslMode` option shown, with this exact key (decision 29):

  | `ssl_mode` | `sslMode` of the save | Arguments added to `pymysql.connect` |
  |---|---|---|
  | `None` | none | none |
  | `DISABLED` | `DISABLED` | `ssl_disabled=True` |
  | `PREFERRED` | `PREFERRED` | none |
  | `REQUIRED` | `REQUIRED` | `ssl`, an `ssl.SSLContext` whose `check_hostname` is `False` and `verify_mode` is `ssl.CERT_NONE` |
  | `VERIFY_CA` | `VERIFY_CA` | `ssl`, an `ssl.SSLContext` whose `check_hostname` is `False` and `verify_mode` is `ssl.CERT_REQUIRED` |
  | `VERIFY_IDENTITY` | `VERIFY_IDENTITY` | `ssl`, an `ssl.SSLContext` whose `check_hostname` is `True` and `verify_mode` is `ssl.CERT_REQUIRED` |

  Given `pymysql.connect` replaced after the writer was built, then the replacement is the one
  called.
- **AT-032**: Given a save that raises `err = RuntimeError("executor lost")`, when `write(df)` or
  `write(df, replace=True)` runs, then it raises `err` itself. The committed statements are, in
  order, the `CREATE` on the first connection and `DROP TABLE IF EXISTS <stg>` on a second one. No
  `INSERT` or `DELETE` is executed on any connection, exactly two connections are opened, and every
  connection is closed.
- **AT-033**: Given a second connection whose cursor raises `err` on the `INSERT`, when
  `write(df)` runs, then it raises `err` itself. That connection commits nothing, executes no
  `DROP TABLE <stg>`, and is closed. A third connection executes `DROP TABLE IF EXISTS <stg>`,
  commits it, and is closed. Given instead a cursor that raises `err` on the `DELETE`, when
  `write(df, replace=True)` runs, then the same holds, and no `INSERT` is executed.
- **AT-034**: Given a second connection whose `commit()` raises `err`, when `write(df)` or
  `write(df, replace=True)` runs, then it raises `err` itself; that connection executes no
  `DROP TABLE <stg>` and is closed; a third connection executes and commits
  `DROP TABLE IF EXISTS <stg>`, and is closed. The same holds when the second connection's
  `begin()` raises `err`, and then no `DELETE` or `INSERT` is executed.
- **AT-035**: Given a first connection whose cursor raises `err` on the `CREATE`, when `write(df)`
  runs, then it raises `err` itself. Only that connection is opened, nothing is committed, the
  connection is closed, and no save is recorded. The same holds when the first `pymysql.connect`
  call raises `err`, with no connection to close.
- **AT-036**: Given a save that raises `err`, and a cleanup whose cursor raises
  `RuntimeError("refused")`, when `write(df)` runs, then it raises `err` itself, and
  `err.__notes__` is
  ``['staging table `shop`.`ingestio_staging_<hex>` could not be dropped: RuntimeError: refused']``.
  The same holds when the cleanup's `pymysql.connect` raises `RuntimeError("refused")`, and when the
  failure is the `INSERT` of AT-033 or the `commit()` of AT-034 instead of the save.
- **AT-037**: Given a second connection whose cursor raises `err` on `DROP TABLE <stg>`, when
  `write(df)` runs, then it raises `err` itself, and `err.__notes__` is
  ``['the rows were committed to `shop`.`orders`, but staging table `shop`.`ingestio_staging_<hex>` could not be dropped']``
  (decision 38). The `INSERT` was committed, the connection is closed, and no other connection is opened
  after it.
- **AT-038**: Given a fake DataFrame with `columns == []`, when `write` runs, with or without
  `replace=True`, then it raises `ValueError("the DataFrame has no columns")`, `pymysql.connect`
  is not called, and no save is recorded.
- **AT-039**: Given a DataFrame with ``columns == ["Id", "my`col", "pct%"]``, when
  `write(df, replace=True)` runs, then the `INSERT` is

  ```text
  INSERT INTO `shop`.`orders` (`Id`, `my``col`, `pct%`) SELECT `Id`, `my``col`, `pct%` FROM <stg>
  ```

  and every statement of the call is executed with no argument besides it, so the `%` reaches
  MySQL as written.

### In a configuration

- **AT-040**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises, when
  `ReadSource(path, connection=conn, spark=spark)` is built with the default `source`, then it
  succeeds, `active` is not called, and no attribute of `spark` is accessed. When
  `Process([Step("extract", [<that ReadSource>])]).run()` runs, then `state["records"]` is the
  DataFrame of the one load recorded, and the events equal those of the twin
  `MySQLReader("shop.orders", conn, spark=<twin session>).read()`. Given the section with
  `"watermark_column": "updated_at"` and `"options": {"fetchsize": 500}` added, and
  `watermark_value=41` given to `ReadSource`, the events equal those of the twin built with the
  same arguments. mypy strict accepts `ReadSource(path, connection=conn)`,
  `AppendOnly(path, connection=conn)`, `FullLoad(path, connection=conn)` and
  `SCD2(path, connection=conn)` (decision 40) with `conn` annotated `MySQLConnection`, a `MySQLReader` bound to a
  variable annotated `Reader[DataFrame]`, and a `MySQLWriter` bound to one annotated
  `Writer[DataFrame]`.
- **AT-041**: Given the default `target` with `"options": {"numPartitions": 4}` added, when
  `AppendOnly(path, connection=conn)` and `FullLoad(path, connection=conn)` are built, then
  `pymysql.connect` is not called, and the writer each built has the state, `vars(...)`, of the twin
  `MySQLWriter("shop.orders", conn, options={"numPartitions": 4})`, its `connection` being `conn`
  itself. Given `MySQLWriter.write` replaced on the class with a recorder, when a process runs each
  callback over `state["records"] = df`, then `AppendOnly` calls `write` once with `df` and no
  keyword argument, and `FullLoad` calls it once with `df` and `replace=True`.
- **AT-042**: Given the section on the left, when the callback of its section is built with
  `connection=conn`, then it raises `ValueError` with the message on the right:

  | Section | Message |
  |---|---|
  | `{"system": "mysql"}` as `source` | `<path>: source.table_name is required for system 'mysql'` |
  | `{"system": "mysql"}` as `target` | `<path>: target.table_name is required for system 'mysql'` |
  | the default `source` with `"host": "db.internal"` | `<path>: source.host is not a key of system 'mysql'` |
  | the same with `"database"`, `"user"`, `"password"`, `"port"`, `"ssl_mode"`, `"connection"`, `"watermark_value"` or `"fetchsize"` as the key | `<path>: source.<key> is not a key of system 'mysql'` |
  | the default `target` with `"watermark_column": "updated_at"` | `<path>: target.watermark_column is not a key of system 'mysql'` |
  | the default `source` with `"table_name": 1` | `<path>: source.table_name must be a string` |
  | the default `source` with `"watermark_column": null` | `<path>: source.watermark_column must be a string` |
  | the default `target` with `"options": {"batchsize": [1]}` | `<path>: target.options must be an object of strings, numbers and booleans` |

- **AT-043**: When `ReadSource(path)` is built with the default `source`, then it raises
  `ValueError("<path>: source system 'mysql' needs the argument connection")`; when `AppendOnly(path)`
  or `FullLoad(path)` is built with the default `target`, then it raises
  `ValueError("<path>: target system 'mysql' needs the argument connection")`. Given
  `client=object()` with `connection=conn`, then `ReadSource` raises
  `<path>: source system 'mysql' takes no argument client`, and `AppendOnly` and `FullLoad` raise
  `<path>: target system 'mysql' takes no argument client`.
  `ReadSource(path, connection=conn, client=None, watermark_value=None, spark=None)` builds.
- **AT-044**: Given the callback, the section and the `connection` on the left, when the callback
  is built, then it raises `ValueError` with the message on the right:

  | Callback | Section | `connection` | Message |
  |---|---|---|---|
  | `ReadSource` | the default `source` | `PostgresConnection("db.internal", "shop", "ingest", "pw")` | `<path>: source system 'mysql' needs a MySQLConnection as the argument connection, not PostgresConnection` |
  | `ReadSource` | the default `source` | `MongoDBConnection("mongodb://db.internal")` | `<path>: source system 'mysql' needs a MySQLConnection as the argument connection, not MongoDBConnection` |
  | `AppendOnly`, `FullLoad` | the default `target` | the `PostgresConnection` above | `<path>: target system 'mysql' needs a MySQLConnection as the argument connection, not PostgresConnection` |
  | `ReadSource` | `{"system": "postgres", "table_name": "public.orders"}` as `source` | `conn` | `<path>: source system 'postgres' needs a PostgresConnection as the argument connection, not MySQLConnection` |
  | `ReadSource` | `{"system": "mongodb", "database": "shop", "collection": "orders"}` as `source` | `conn` | `<path>: source system 'mongodb' needs a MongoDBConnection as the argument connection, not MySQLConnection` |
  | `AppendOnly`, `FullLoad` | `{"system": "postgres", "table_name": "public.orders"}` as `target` | `conn` | `<path>: target system 'postgres' needs a PostgresConnection as the argument connection, not MySQLConnection` |

  A `ReadSource` and an `AppendOnly` given an instance of a subclass of `MySQLConnection` build.
- **AT-045**: Given a `source` whose `system` is `"MySQL"`, when `ReadSource(path)` is built, then
  it raises
  `ValueError("<path>: source.system 'MySQL' is not one of: bigquery, disk, gcs, json_api, mongodb, mysql, mysql_cdc, postgres, s3, unity_catalog, volume")`.
  Given a `target` whose `system` is `"mariadb"`, when `AppendOnly(path)` is built, then it raises
  `ValueError("<path>: target.system 'mariadb' is not one of: bigquery, disk, gcs, json_lines, mysql, postgres, s3, unity_catalog, volume")`.
- **AT-046**: Given the section on the left, when the callback of its section is built with
  `connection=conn`, then it raises the error on the right, the same object that the connector
  raises, with exactly one note, `while building the <section> of <path>, system 'mysql'`:

  | Section | Error |
  |---|---|
  | the default `source` with `"table_name": "a.b.c"` | `ValueError("table_name must be 'table' or 'database.table'")` |
  | the default `source` with `"options": {"dbtable": "x"}` | `ValueError("option 'dbtable' is set by the connector")` |
  | the default `target` with `"options": {"sslMode": "DISABLED"}` | `ValueError("option 'sslMode' is set by the connector")` |
  | the default `target`, in an interpreter where `pymysql` is mapped to `None` in `sys.modules` | the `ImportError` of AT-003 |

  In that interpreter, `ReadSource(path, connection=conn)` with the default `source` builds.

### `SCD2`

- **AT-047**: Given the file
  `{"target": {"system": "mysql", "table_name": "shop.dim_customer"}, "scd2": {"key_columns": ["customer_id"], "effective_time_column": "updated_at"}}`,
  when `SCD2(path, connection=conn)` is built, then it succeeds and `pymysql.connect` is not called
  (decision 40). `SCD2(path)` raises
  `ValueError("<path>: target system 'mysql' needs the argument connection")`, and
  `SCD2(path, connection=<the PostgresConnection of AT-044>)` raises
  `ValueError("<path>: target system 'mysql' needs a MySQLConnection as the argument connection, not PostgresConnection")`.
  Given the `target` `{"system": "disk", "path": "/data/dim", "format": "parquet"}` instead, then
  `SCD2(path)` raises
  `ValueError("<path>: target.system 'disk' is not one of: bigquery, mysql, postgres, unity_catalog")`.
- **AT-048**: Given the file of AT-047, a fake batch in `state["records"]` whose `columns` are
  `["customer_id", "name", "city", "updated_at"]` and whose `sparkSession` is a fake session, and
  `MySQLReader.read` replaced on the class with a function that records the reader and raises
  `err`, when a process runs `SCD2(path, connection=conn)`, then it raises `err` itself; the
  recorded reader has the state, `vars(...)`, of the twin
  `MySQLReader("shop.dim_customer", conn, spark=<the batch's session>)`; and the writer's `write`
  is not called.

### Logging and secrets

- **AT-049**: Given `caplog` capturing every level, when connections, readers, writers and the
  callbacks of AT-040 to AT-048 are built, `read()` runs as a full read, as a watermark read and with a
  bound of `None`, and `write` runs successfully, after a failed save, and with a failed cleanup,
  then no log record is captured.
- **AT-050**: Given the connections, connectors and callbacks that AT-005 to AT-048 build, then
  `s3cret` appears in no `repr` or `str` of them, in no option `url`, and in the message or the
  notes of no error those criteria raise.

## Decisions

Taken by the maintainer on 2026-10-24, before planning. Their rationale is the technical reason the
maintainer gave; they are recorded here and not reopened.

1. **Two specs; the CDC reader in a later one; applying the events is a later callback.** This
   spec covers `MySQLConnection`, `MySQLReader`, `MySQLWriter`, the `mysql` system and the `mysql`
   extra. The later spec `mysql_cdc` covers a CDC reader of the binary log, which reuses
   `MySQLConnection`. Applying change events to a target is a callback with a spec of its own.
   - Rationale: the philosophy lists CDC replication as a callback, and
     [ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md) makes each write
     strategy one callback, so the connectors only talk to MySQL.
2. **The CDC mechanism, for the later spec: the binary log, read on the Spark driver with
   `python-mysql-replication`.** Recorded here only because it bears on this spec's client
   package: P1 and decision 24.
   - Rationale: it captures deletes, which a watermark misses.
3. **Writer atomicity: a staging table and a `DELETE` in one transaction.** Spark loads a staging
   table in parallel; then, in one transaction on the driver: `DELETE FROM <target>` when
   `replace=True`, then `INSERT INTO <target> (<cols>) SELECT <cols> FROM <staging>`, and commit;
   the staging table is dropped afterwards, outside the transaction, and on failure, as
   `PostgresWriter` drops it.
   - Rationale: in MySQL, `CREATE TABLE`, `DROP TABLE`, `TRUNCATE TABLE` and `RENAME TABLE` commit
     the open transaction implicitly (M1), so `PostgresWriter`'s `TRUNCATE` inside the transaction
     does not carry over. `DELETE` keeps the target's grants, its triggers and the foreign keys
     that reference it, which a `RENAME TABLE` swap would lose, and needs no `CREATE` or `DROP`
     privilege on the target.
   - Its cost, a known limitation: `DELETE` is slow on large tables and grows the undo log.

Followed from what the shipped connectors settled. Each follows the precedent named with it and is
not an open question:

4. **Spark moves the data.** Spark's JDBC data source reads and writes on the executors, with MySQL
   Connector/J as a prerequisite JAR, as the PostgreSQL JDBC driver is; a native client on the
   driver only runs the transaction ([PostgreSQL spec](postgres_connectors.md), decisions 1 and 4;
   [backlog](../docs/backlog.md), "Shared by ideas 8 and 9").
5. **The job names the system in its configuration:** one more row of the closed table in
   `src/ingestio/callback/_configuration.py`, for the source and the target; section keys are the
   constructor arguments of the same name; credentials and per-run values are callback arguments
   ([ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md)).
6. **The writer offers `write(data, *, replace=False)`** and decides nothing else; `AppendOnly`
   appends and `FullLoad` replaces
   ([ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md)).
7. **The target must already exist;** the writer runs no DDL on it
   ([ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md),
   as for `PostgresWriter`, decision 29 of its spec).
8. **The callbacks check the type of `connection` per system**, which widens `connection` of
   `ReadSource`, `AppendOnly` and `FullLoad` (and `SCD2`, decision 40) to `MySQLConnection`
   ([specs/mongodb_reader.md](mongodb_reader.md), decision 10, and the consequence of ADR 0015
   that a later system with credentials follows the same pattern).
9. **ADR 0012 gets a "leans towards batch" bullet** for these connectors, naming the CDC reader as
   a different connector (see [ADRs](#adrs)).
10. **The reader's recipe is `PostgresReader`'s:** the filter written into the SQL that MySQL runs,
    in a `dbtable` subquery (its decision 8); the slice frozen by an upper bound that one `max()`
    query captures as MySQL's text (decision 10); the comparison strictly greater, and `NULL`
    watermarks never read (decision 23); the five watermark value types, naive datetimes and
    floats rejected, aware datetimes normalised to UTC (decisions 9 and 24); one `options` mapping
    passed through, with the connector's keys reserved, and no `query=`, `columns=` or
    `primary_key` (decisions 25 and 26); names always quoted (decision 28); an empty DataFrame
    through the same sequence (decision 30). Only the literals and the quoting change, for MySQL
    (decisions 33 and 34).
11. **The extras pattern** ([PostgreSQL spec](postgres_connectors.md), decision 19): an extra named
    after the system; lazy imports; an `ImportError` naming the extra when a connector that needs
    the package is built; pyspark and the JAR are prerequisites; the `dev` group holds the extra's
    packages.
12. **Tests with doubles, and opt-in integration tests.** Every criterion runs with test doubles.
    The tests that need Spark are opt-in through `INGESTIO_SPARK_TESTS=1` and run in CI, as in the
    other Spark connector specs ([specs/scd2_callback.md](scd2_callback.md), decision 34); the
    development machine cannot run Spark. Here only the integration tests need Spark (decision 43).

Proposed by this spec and accepted by the maintainer on 2026-10-24, as written. Each gives its
technical reason or the precedent it follows:

13. **Building does nothing but validate,** and each `read()` looks up the session and loads
    again, as the PostgreSQL spec (decision 18) does, so that one reader serves every run.
14. **The generic `jdbc` format,** not a Databricks-only named format, as the PostgreSQL spec
    (decision 7) chose: it works on open-source Spark and every Databricks Runtime.
15. **`MySQLConnection` builds the PyMySQL keyword arguments in one private place,** as
    `PostgresConnection._connect_arguments` does for psycopg: the writer uses it, and the CDC
    reader of the later spec can hand the same arguments to
    `BinLogStreamReader(connection_settings=...)`, which calls `pymysql.connect(**connection_settings)`.
    It is not public API, so nothing is added for the CDC reader and nothing closes its door.
16. **The connect arguments carry `charset="utf8mb4"`.** The writer's statements carry column and
    table names, which may hold any Unicode character, and `utf8mb4` encodes all of them; the
    reader's literals are `_utf8mb4` strings for the same reason.
17. **Statements are executed without arguments,** `cursor.execute(<statement>)`, so that PyMySQL
    formats nothing (P6) and a `%` in a name reaches MySQL as written; names are quoted by the
    connector with backticks (S2, M2), and values never appear in the writer's statements.
18. **The writer calls `begin()` before the apply, `commit()` after each statement group, and
    `close()` in every case,** and never relies on PyMySQL's context manager, whose behaviour on
    exit this spec does not establish. `begin()` opens the transaction whatever the session's
    `autocommit`.
19. **No PyMySQL connection is open while Spark writes,** as the PostgreSQL spec (decision 15)
    gives the reason: long writes would run into idle timeouts.
20. **The cleanup runs on a new connection, and its failure becomes a note** on the original
    exception, as the PostgreSQL spec (decision 16) does: raising the cleanup error would hide the
    cause. The connectors write no log lines, because a connector does not know the run id.
21. **The writer evaluates the DataFrame once,** through the save, and never counts it, as the
    PostgreSQL spec (decision 17).
22. **Errors propagate unchanged.** The connectors catch nothing to carry on.
23. **The bound query also returns the column's maximum, typed,** so that the reader learns from
    its schema whether the column is numeric (decision 34), with no second query.

Taken by the maintainer on 2026-10-24 when answering the 20 open questions of the first draft,
each with the recommended option. They are numbered after decision 23 so that the earlier numbers
do not change: question *n* became decision 23 + *n*. The rationale of each is the technical reason
of the recommendation, which the maintainer took as written.

24. **The client package on the driver is PyMySQL** (question 1, option (a)).
    - Rationale: `python-mysql-replication`, the client of the later CDC reader (decision 2),
      requires `pymysql>=1.1.0` (P1), so PyMySQL keeps one MySQL client in the extra, where
      mysql-connector-python would add a second one.
25. **The extra is `mysql = ["PyMySQL[rsa]>=1.2.0"]`** (question 2, option (a)).
    - Rationale: from 1.2.0, PyMySQL without an ssl argument tries TLS and falls back (P3), as
      Connector/J does by default (J1), so `ssl_mode=None` and `PREFERRED` mean the same on the
      executors and on the driver; at 1.1.x the driver would connect without TLS where the
      executors use it. The `rsa` extra installs `cryptography` (P5), which PyMySQL needs for the
      full authentication of `caching_sha2_password` over a connection without TLS (P5), MySQL
      8.4's default plugin (K7): without it, `ssl_mode="DISABLED"` fails at the first full
      authentication. That is the only reason for `rsa`.
26. **Names: `MySQLConnection`, `MySQLReader` and `MySQLWriter`; the system `mysql`; the module
    `src/ingestio/connector/mysql.py`; the extra `mysql`** (question 3, option (a)).
    - Rationale: the product's name read as one word gives the system name, as `BigQueryReader`
      gives `bigquery` and `MongoDBReader` gives `mongodb` (MongoDB spec, decision 6), and the
      class keeps the product's capitals, as PEP 8 asks for acronyms (`SQL`).
27. **`MySQLConnection(host, database, user, password, *, port=3306, ssl_mode=None)`, with
    `database` required** (question 4, option (a)).
    - Rationale: it is `PostgresConnection`'s shape, an unqualified `table_name` always resolves to
      one database, and an unqualified target's staging table has a database to live in. The later
      CDC reader can leave `database` out of what it hands to `BinLogStreamReader` if it needs to.
      The TLS argument is decision 29.
28. **`table_name` is `table` or `database.table`,** with the message
    `table_name must be 'table' or 'database.table'` (question 5, option (a)).
    - Rationale: MySQL's schema is the database, so `database.table` is MySQL's own qualified name,
      and the two forms are `PostgresReader`'s; a job can reach another database on the same server
      with the same credentials.
29. **TLS: `ssl_mode: str | None = None`,** with Connector/J's five values (J1), sent as `sslMode`
    and translated for PyMySQL as the table of [`MySQLConnection`](#mysqlconnection) says
    (`ssl_disabled=True`, nothing, or an `ssl.SSLContext`); CA material from each client's default
    trust store, and a private CA through Connector/J's trust-store options and Python's
    `SSL_CERT_FILE` (K8) (question 6, option (a)).
    - Rationale: one mode for both clients is what keeps the executors and the driver in the same
      TLS mode; an `ssl.SSLContext` is used as given at every PyMySQL release this spec supports
      (P3), where `ssl={}` or `ssl_verify_cert=False` alone enable nothing at 1.1.x. The CA material
      differs between the clients (J1, P3), so one CA argument cannot serve both, and a PEM file for
      PyMySQL would configure the driver only. A CA argument can be added later without closing a
      door, for the CDC reader too.
30. **Read defaults: `useCursorFetch` = `true` and `fetchsize` = `10000`,** which the user's
    `options` replace (question 7, option (a)).
    - Rationale: Spark's MySQL dialect sets no fetch size (S4), and a fetch size alone has no effect
      without `useCursorFetch` (J2), so without them each partition is read whole into an
      executor's memory; they are the counterpart of `PostgresReader`'s `fetchsize` of 10000.
      Connector/J's row-by-row streaming would depend on Spark accepting a negative `fetchsize`,
      which this spec does not establish. The cost: the server materialises each partition's result
      (K4, to be confirmed).
31. **Write default: `rewriteBatchedStatements` = `true`,** which the user's `options` replace
    (question 8, option (a)).
    - Rationale: it is the counterpart of `reWriteBatchedInserts`, which `PostgresWriter` sets
      (J3), and turns Spark's batches into multi-row `INSERT`s.
32. **Reserved options: `url`, `dbtable`, `query`, `user`, `password` and `driver`, plus `sslMode`,
    `useSSL`, `requireSSL` and `verifyServerCertificate`;** the connector's defaults, the time-zone
    options included, pass through (question 9, option (a)).
    - Rationale: the six keys come from the connector's arguments, as on the PostgreSQL connectors.
      A user's `sslMode` or legacy TLS key (J1) would reach the executors only, so the writer's two
      clients would run in different TLS modes, which `ssl_mode` exists to prevent. The time-zone
      options stay replaceable, as `PostgresReader`'s defaults are, for a job whose server needs
      another setting; replacing them gives up the guarantees of decision 35.
33. **Watermark values: numbers as numeric literals (`41`, `12.50`); text, dates and datetimes as
    hex string literals with the `utf8mb4` introducer, `H(text)`** (question 10, option (a)).
    - Rationale: a hex string literal means the same in every `sql_mode` (M3), and the introducer
      makes it a character string in a known character set rather than a binary string. Doubling
      both `'` and `\` is safe in every mode but changes a value that holds `\` under
      `NO_BACKSLASH_ESCAPES`; doubling `'` alone, as `PostgresReader` does, lets a value end the
      literal early in the default mode, an injection (M3). Numbers stay numeric because MySQL
      compares a string with a number as floating-point numbers (K1).
34. **The bound: the bound query also returns `max(<col>)` itself; when Spark's type for it is
    numeric and the text is a number, the bound is written unquoted, otherwise as `H(text)`**
    (question 11, option (a)).
    - Rationale: an integer or decimal bound is then compared exactly, so the slice stays frozen
      for any key, `BIGINT` identifiers above 2^53 included (K1); the type comes with the bound
      query, at no extra query, and the regular expression keeps every non-numeric text quoted. Its
      cost: Spark converts the typed maximum too when `first()` fetches the row. MySQL's own name
      for the type, through `JSON_TYPE`, would rest on JSON conversions that this spec does not
      establish.
35. **Every JDBC session of the reader and of the writer carries `connectionTimeZone=UTC` and
    `forceConnectionTimeZoneToSession=true`** (J4) (question 12, option (b)).
    - Rationale: the bound and the data load then run in the same zone, UTC, which has no repeated
      hour (M4); an aware datetime compares with a `TIMESTAMP` column as the same instant; and a
      `TIMESTAMP` value read and written back, as `SCD2` does on every run, keeps its instant
      whatever the server's default `time_zone`, where with Connector/J's defaults the values cross
      as if the session ran in the JVM's zone (J4). Setting them on the reader only would leave the
      writer exposed to that shift; offsets in the literals would rest on K16 and still leave the
      bound ambiguous in a zone with daylight saving time.
36. **The staging table is created with `CREATE TABLE <stg> LIKE <target>`** (M5) (question 13,
    option (b)).
    - Rationale: a server with `sql_require_primary_key=ON`, or under Group Replication, rejects a
      table without a primary key (K3, to be confirmed), which `CREATE TABLE … AS SELECT` creates;
      `LIKE` copies the target's primary key, and makes the load apply the target's column rules
      before the apply's transaction starts. Its costs: the indexes slow the parallel load, and a
      unique key turns a re-run Spark task into a duplicate-key failure. A primary key column of the
      connector's would add a column to the mechanism and a name that can collide with a
      DataFrame's column.
37. **The staging table is `ingestio_staging_<uuid4 hex>`, in the target's database** (question
    14, option (a)).
    - Rationale: the name is recognisable for manual cleanup and unique without the run id, which
      connectors do not know, and fits MySQL's 64 characters (K13); the PostgreSQL spec rejected a
      `staging_schema` (its decision 13), and a second database would need privileges there too.
38. **A `DROP` of the staging table that fails after the commit propagates,** with one note:
    `the rows were committed to <target>, but staging table <stg> could not be dropped` (question
    15, option (a)).
    - Rationale: `BigQueryWriter` does not raise because its staging table expires within a day; a
      MySQL table never expires, so not raising would leave a full copy of the batch in the
      database with nothing that reports it. The note tells the job that the rows are in the
      target, so that it does not append them again. Running the cleanup again would add a retry
      path for a rare failure.
39. **A target on a storage engine without transactions is a documented limitation;** the writer
    checks nothing (question 16, option (a)).
    - Rationale: the job chose the target's engine, and the philosophy keeps architecture out of
      connectors; a check would need one more query in every write and a list of transactional
      engines kept by the connector, NDB or a future engine included.
40. **`mysql` is an `SCD2` target,** and `SCD2`'s `connection` widens to `MySQLConnection`
    (question 17, option (a)).
    - Rationale: `MySQLWriter` meets the rule of decision 26 of the SCD2 spec: its
      `write(data, replace=True)` is atomic and evaluates `data` whole into the staging table
      before the transaction touches the target, and `MySQLReader`'s full read is one statement.
      Its cost is decision 3's: every run deletes and inserts every row of the dimension.
41. **Supported versions: MySQL 8.4 LTS and 9.x; Connector/J from 8.0.23; MariaDB not supported**
    (question 18, option (a)).
    - Rationale: MySQL 8.0 reached its end of life (M6, to be confirmed); Connector/J 8.0.23 is the
      first release with the time-zone properties of decision 35 (J4); MariaDB's own URL is not
      matched by Spark's MySQL dialect (S1), and MySQL Connector/J against MariaDB is not
      established.
42. **`python-mysql-replication` is not in the extra now;** the CDC spec decides whether it joins
    the `mysql` extra or gets one of its own (question 19, option (a)).
    - Rationale: nothing in this spec imports it, and the pattern installs a package only for a
      connector that imports it (rule 3 of the extras); P4 shows that the version floor depends on
      the MySQL versions that spec supports.
43. **Two tiers of opt-in integration tests,** as [Integration tests](#integration-tests)
    describes: tier 1 with `INGESTIO_MYSQL_URL` and no Spark; tier 2 also with
    `INGESTIO_SPARK_TESTS=1` and `INGESTIO_MYSQL_JDBC_PACKAGE`. CI runs them once the maintainer
    adds a MySQL 8.4 service and the two variables to `.github/workflows/ci.yml` (question 20,
    option (a)).
    - Rationale: several rules rest on the K facts, which no source read for this spec
      establishes, and tier 1 checks the writer's SQL against a real server without Spark, so it can
      run wherever a MySQL server can, while tier 2 checks the literals and the time zones through
      Connector/J.

### ADRs

- **No decision changes the philosophy, and none closes the door to streaming.** `Reader` and
  `Writer` are unchanged, and their payload stays open. No new ADR is needed.
- **ADR 0012, amended.** One more bullet in "What leans towards batch": the MySQL connectors lean
  towards batch: `MySQLReader.read()` returns one slice, with a watermark fixed when the reader is
  built, and `MySQLWriter.write()` needs the whole DataFrame before its transaction commits. A CDC
  source for MySQL, reading the binary log from a position, is a different connector, the CDC
  reader of the later spec `mysql_cdc`. The doc-keeper writes it.
- **ADR 0015, amended.** Its Decision lists the system names, which gain `mysql`, a source and a
  target. Its consequence on `connection` records that `ReadSource`, `AppendOnly`, `FullLoad` and
  `SCD2` take a `MySQLConnection` for `mysql`, the pattern it announced for backlog idea 8, and
  that the type check covers it. The doc-keeper writes both, for the maintainer's approval.
- **ADR 0014 applies unchanged;** its consequences can name `MySQLWriter` among the writers that
  create nothing, its staging table being the writer's own mechanism.
- **ADR 0002 applies unchanged:** the `mysql` extra follows the pattern.

## Open questions

None open. On 2026-10-24 the maintainer answered the 20 questions of the first draft, each with
the recommended option, and accepted the proposed decisions 13 to 23 as written. The answers are
decisions 24 to 43:

| Question | Answer | Decision |
|---|---|---|
| 1. The client package on the driver | (a) PyMySQL | 24 |
| 2. PyMySQL's minimum version and the extra's content | (a) `PyMySQL[rsa]>=1.2.0` | 25 |
| 3. Names | (a) `MySQLConnection`, `MySQLReader`, `MySQLWriter`, `mysql` | 26 |
| 4. The arguments of `MySQLConnection` | (a) `database` required | 27 |
| 5. The shape of `table_name` | (a) `table` or `database.table` | 28 |
| 6. TLS | (a) `ssl_mode`, translated for both clients | 29 |
| 7. Read defaults | (a) `useCursorFetch` and `fetchsize` 10000 | 30 |
| 8. Write defaults | (a) `rewriteBatchedStatements` | 31 |
| 9. Reserved options | (a) the six keys and the four TLS keys | 32 |
| 10. The literals of watermark values | (a) numeric literals and `H(text)` | 33 |
| 11. The literal of the bound | (a) typed by the bound query's schema | 34 |
| 12. `TIMESTAMP` and the session time zone | (b) UTC on the reader's and the writer's sessions | 35 |
| 13. How the staging table is created | (b) `CREATE TABLE … LIKE` | 36 |
| 14. The staging table's name and place | (a) `ingestio_staging_<hex>` in the target's database | 37 |
| 15. A `DROP` that fails after the commit | (a) propagates, with a note | 38 |
| 16. A storage engine without transactions | (a) documented, not checked | 39 |
| 17. `mysql` as an `SCD2` target | (a) yes | 40 |
| 18. Supported versions | (a) MySQL 8.4 and 9.x, Connector/J 8.0.23, no MariaDB | 41 |
| 19. `python-mysql-replication` in the extra | (a) not now | 42 |
| 20. Integration tests and CI | (a) two opt-in tiers | 43 |

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-24 | planning | draft | Drafted from the maintainer's decisions of 2026-10-24 (two specs, with the CDC reader and the apply callback later; the binary log read with `python-mysql-replication`; a staging table plus `DELETE` and `INSERT … SELECT` in one transaction) and the source facts S1–S4, J1–J5, P1–P4 and M1–M6 from the main session: `MySQLConnection`, `MySQLReader` with a frozen upper bound and literals that hold in every `sql_mode`, `MySQLWriter` on PyMySQL, the `mysql` source and target systems, `connection` widened in the four callbacks, `mysql` as an SCD2 target, and the `ingestio[mysql]` extra. 50 criteria that run with test doubles and two tiers of opt-in integration tests. 23 decisions: 3 from the maintainer, 9 that follow earlier specs, 11 proposed. 20 open questions. K1–K16 are the planner's own knowledge, still to be confirmed. |
| 2026-10-24 | planning | draft | Revised with the maintainer's answers: the recommended option of all 20 questions became decisions 24–43 (question n is decision 23 + n), and the proposed decisions 13–23 were accepted as written. The facts the main session read in the source replace K12, K15 and the PyMySQL part of K7: J6 (Connector/J `URLDecoder` and trim of the URL path), P5 (`cryptography` for `caching_sha2_password` without TLS, the `rsa` extra), P6 (`mogrify` with `args=None`), P7 (no `py.typed`, typeshed `types-PyMySQL` 1.2.*). `types-PyMySQL>=1.2` joins the `dev` group. Criteria changed only in their tags, now decision references. |
| 2026-10-24 | approval | approved | Pre-approved by the maintainer on 2026-10-24, on the condition that the revision only records the answers and the verified facts. The main session checked the revision: 50 criteria, no question tags left, decisions 24–43 match the answers; the extra lines are the record of J6's trim, citation updates and option letters replaced by descriptions, with no change of behaviour, criteria or scope. |
| 2026-10-24 | tests | tested | The test-writer wrote `tests/test_mysql_connectors.py`: 88 tests and 225 cases covering AT-001 to AT-050, with a fake Spark session, fake DataFrames and a fake `pymysql.connect` that follows PyMySQL 1.2.3, plus the two opt-in tiers of decision 43 (8 tier-1 and 9 tier-2 cases, skipped here for the reasons the spec gives). The new tests fail on the missing `MySQL*` classes and the missing `mysql` system; AT-004 already passes from `pyproject.toml`. The lists of systems and the unknown names (`mysql` became `mariadb`) were updated in the tests of `config_connectors`, `append_only_callback`, `mongodb_reader` and `scd2_callback`, whose 26 failing cases differ only by `mysql` in the lists. Checked by the main session: full suite 61 failed, 1675 passed, 140 skipped, 172 errors, all in those five files; only `tests/` changed. |
| 2026-10-24 | documentation | documented | The doc-keeper documented the MySQL connectors: a full section in `docs/concepts/connectors.md` (connection with the `ssl_mode` translation for both clients, names, reader, literals, time zones, options, writer, example, environments and privileges, limitations) and the `mysql` extra; the `mysql` system, `connection` widened to `MySQLConnection` and `mysql` as an `SCD2` target in `configuration.md` and `callbacks.md`; ADRs 0012 (a leans-towards-batch bullet naming `mysql_cdc` as a different connector), 0014 and 0015 amended, with no new `TODO(maintainer)`; `docs/index.md` and the architecture KB. The configuration example now uses `mariadb` as the unknown system. The doc-keeper had no shell; the main session ran the documentation examples: 28 passed, and 2 fail only on the missing `MySQLConnection` (connectors.md lines 1984 and 2255). |
| 2026-10-24 | implementation | implemented | The implementer added `src/ingestio/connector/mysql.py` with `MySQLConnection` (a frozen dataclass; the Connector/J options and the PyMySQL connect arguments built in two private methods, a new SSL context on each call), `MySQLReader` (a watermark read freezes its upper bound in a first query and types it from that query's schema; backtick-quoted names, hex or numeric literals) and `MySQLWriter` (`CREATE TABLE … LIKE`, the JDBC save into the staging table, one PyMySQL transaction with `DELETE` and `INSERT … SELECT`, then the `DROP`, with the cleanup and its notes). They are exported from `ingestio.connector.all` only. The `mysql` source and target rows and the `SCD2` target are in `callback/_configuration.py`, and `connection` is widened in the four callbacks in `callback/io.py`. Beyond the letter of the spec: a local helper runs the writer's setup and cleanup statements, and `MySQLReader` checks an empty `watermark_column` before a `watermark_value` without one, as `PostgresReader` does. Checked by the main session: 1910 passed, 140 skipped (the opt-in tests), ruff, format, mypy strict and `uv lock --check` clean; only `src/` changed. |
| 2026-10-24 | validation | validated | The acceptance-validator passed all seven checks: AT-001 to AT-050 in 208 passing cases, the full suite 1910 passed and 140 opt-in skips, all 30 documentation examples, ruff, format, mypy strict and `uv lock --check` clean. Read against the spec's Behaviour sections (TLS for both clients, options and defaults, literals and the typed bound, UTC sessions, the staging table, the writer's sequence, failures and notes, the connect arguments in one private place, secrets), `mysql.py` diverges nowhere, and both points the implementer flagged are acceptable. The 17 integration cases of decision 43 were not run: no MySQL server and no JVM here, and CI has no MySQL service yet, so K1–K16 stay unconfirmed. Non-blocking notes: AT-030 of `mongodb_reader` (amended by the main session, recorded in that spec), the backlog's status line (updated), a broken code span at `docs/concepts/connectors.md:2092` and unwrapped lines in `callbacks.md`, `configuration.md` and ADR 0015, an unused `begin` keyword in the writer's local helper, and the README row. |
| 2026-10-24 | return | tested | Returned by the main session at the maintainer's request ("Vamos resolver as pendencias"), to settle the validator's non-blocking notes: to the doc-keeper for the broken code span at `docs/concepts/connectors.md:2091` and the inserted lines left unwrapped in `callbacks.md`, `configuration.md` and ADR 0015, then to the implementer for the unused `begin` keyword of the writer's local helper in `mysql.py`. |
| 2026-10-24 | documentation | documented | After the return, the doc-keeper fixed the nested-backtick span of the watermark-read bullet in `docs/concepts/connectors.md` (now ``` `` WHERE `col` > <value> `` ```) and re-wrapped at 100 columns the prose lines it had inserted in `connectors.md`, `callbacks.md`, `configuration.md` and ADR 0015; the comment of `SCD2`'s example in `callbacks.md` now carries the full message on its own line. No wording changed. Checked by the main session: 30 documentation examples pass, and no added prose line runs past 100 columns. |
| 2026-10-24 | implementation | implemented | After the return, the implementer removed the unused `begin` keyword, and its branch, from the writer's local helper in `mysql.py`; the apply keeps its own `begin()` on its one connection. Nothing else changed. Checked by the main session: 1910 passed, 140 skipped, ruff, format, mypy strict and `uv lock --check` clean. |
| 2026-10-24 | validation | validated | The acceptance-validator passed all seven checks again after the return: AT-001 to AT-050 in 208 passing cases, the full suite 1910 passed and 140 opt-in skips, all 30 documentation examples, ruff, format, mypy strict and `uv lock --check` clean. The five non-blocking notes are settled (the `begin` keyword removed, the code span fixed, the inserted lines re-wrapped, AT-030 of `mongodb_reader` amended, the backlog updated) and nothing else changed. The 17 integration cases of decision 43 remain not run: no MySQL server and no JVM here. |
| 2026-10-24 | amendment | validated | Amended by the main session, as [mysql_cdc](mysql_cdc.md) (What becomes outdated) asks, approved on 2026-10-24: `mysql_cdc` becomes a source system there, so the list of source systems in AT-045 and under In a configuration gains `mysql_cdc`. What the criterion checks is unchanged; the test-writer of `mysql_cdc` updates `SOURCE_NAMES` in `tests/test_mysql_connectors.py`. The Related line now links the CDC spec. |
| 2026-10-24 | test change | validated | Recorded by the main session: the test-writer of [mysql_cdc](mysql_cdc.md) changed `test_at004_...` in `tests/test_mysql_connectors.py` to look for the `PyMySQL` requirement whose marker is `extra == "mysql"`, as AT-004 reads, instead of requiring a single `PyMySQL` requirement in the whole metadata, which the `mysql-cdc` extra broke. The criterion is unchanged. |
