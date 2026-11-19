# Spec: Snowflake reader and writer connectors

- **Status:** validated
- **Slug:** `snowflake_connectors`. The spec is `specs/snowflake_connectors.md` and its tests are
  `tests/test_snowflake_connectors.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md),
  [ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md),
  [ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md),
  [ADR 0017](../docs/decisions/0017-read-a-library-callbacks-own-parameters-from-a-section-named-after-it.md),
  [specs/mysql_connectors.md](mysql_connectors.md) (the closest precedent),
  [specs/mongodb_reader.md](mongodb_reader.md) (a connector JAR and no extra),
  [specs/postgres_connectors.md](postgres_connectors.md),
  [specs/config_connectors.md](config_connectors.md),
  [specs/append_only_callback.md](append_only_callback.md),
  [specs/scd2_callback.md](scd2_callback.md),
  [Connectors](../docs/concepts/connectors.md),
  [Configuration](../docs/concepts/configuration.md),
  [Callbacks](../docs/concepts/callbacks.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

Two connectors for Snowflake, built on the Snowflake Connector for Spark
(`net.snowflake.spark.snowflake`, format `snowflake`), and the value that says how to reach a
Snowflake database. `SnowflakeReader` returns a Spark DataFrame over one table: all of it, or, with a
watermark column, only the rows above the job's last watermark, filtered inside Snowflake over a
slice that `read()` freezes with an upper bound, as `MySQLReader` does. `SnowflakeWriter` appends a
DataFrame to a table that already exists, or replaces the table's rows with it, atomically, through
the connector's own transaction: the connector stages the rows, then runs `START TRANSACTION`, a
`TRUNCATE` on a replace, `COPY INTO` and `COMMIT`, and rolls back on any failure.
`SnowflakeConnection` holds the account's host name, the database, the optional schema, warehouse
and role, and one of two credentials: an unencrypted private key, or an OAuth access token.

The job names the system `snowflake` in its configuration, as a source and as a target. The
`connection` argument of `ReadSource`, `AppendOnly`, `FullLoad` and `SCD2` widens to take a
`SnowflakeConnection`, and the callbacks check its type. `snowflake` is an `SCD2` target. No extra is
added: the connectors import no Python package besides pyspark, and the connector's JAR and the
Snowflake JDBC driver are prerequisites that the runtime provides.

The maintainer took four decisions on 2026-11-18 ([Decisions](#decisions) 1 to 4). Decisions 5 to
12 follow the shipped connectors. The same day the maintainer accepted the decisions this spec
proposed (13 to 20) as written, and answered the thirteen questions of the first draft: the
recommended option of questions 1 to 12, and option (c) of question 13, which V4 settles (decisions
21 to 33). A rule or a criterion that rests on one of these decisions names it. The maintainer then
answered question 14, raised by V1, with its option (a) (decision 34).

## Motivation

- **A Snowflake source and target.** The library integrates no Snowflake system, and since
  [ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md) a job reaches a
  system only through a connector the library ships: a job that reads from or writes to Snowflake
  through `ReadSource`, `AppendOnly`, `FullLoad` or `SCD2` needs one more row of the closed table.
- **Incremental reads, filtered in the source.** Jobs ingest tables by a watermark column such as
  `updated_at`, and [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md)
  requires a reader to filter inside the source.
- **Laziness.** A DataFrame read through the connector runs its query in Snowflake again at every
  action. Without an upper bound, a job that counts the rows, writes them and computes its next
  watermark from the same DataFrame can see different rows each time, and record a watermark above
  rows it never wrote. `PostgresReader` and `MySQLReader` capture a bound for this reason.
- **Atomic writes.** With its default options, the connector writes an overwrite through a staging
  table that it swaps with the target, recreating the column types from the DataFrame, and an append
  with no `START TRANSACTION` (F4). Its other path runs the truncation and the load inside one user
  transaction and rolls both back on failure (F2, F3).
- **A missing target.** That transactional path creates a missing table from the DataFrame's schema
  before its transaction (F3). Creating a Snowflake table from a DataFrame needs a type mapping
  chosen by the connector, which
  [ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md)
  forbids, so the writer must make a missing target fail (decision 2).
- **Credentials.** Snowflake turns off single-factor password sign-in for all users in the final
  phase of its rollout, August to October 2026, `LEGACY_SERVICE` users included (F13). A job reads
  its key or its token from a secret store at run time, `dbutils.secrets.get` on Databricks; the
  library never reads a secret.
- **The case of names.** Snowflake folds unquoted identifiers to upper case, and the connector uses
  `dbtable` verbatim, with no escaping (F10). The connectors must say how a name in the
  configuration reaches the SQL, and keep any other text out of it.

## Fit with the philosophy

- **Concept: connector.** `SnowflakeReader` satisfies `Reader[DataFrame]` and `SnowflakeWriter`
  satisfies `Writer[DataFrame]`, structurally, with no base class and no registration
  ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)). `ReadSource`, `AppendOnly`,
  `FullLoad` and `SCD2` build them from the job's configuration
  ([ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md)), through one
  more row of the closed table, which stays closed.
- **No third concept.** `SnowflakeConnection` is a value, like `PostgresConnection` and
  `MySQLConnection`: it groups what reaching one database needs, opens nothing and performs no
  action.
- **The connectors decide no strategy.** `write(data, *, replace=False)` offers the two primitive
  ways to put a batch into a table, `COPY INTO`, and `TRUNCATE` followed by `COPY INTO`, and picks
  neither: `AppendOnly` appends, `FullLoad` and `SCD2` replace
  ([ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md)). The options that
  route the connector to its transaction are how the writer makes a write all-or-nothing, a
  mechanism, not a strategy. Whether a run is incremental, and from which value, is the job's
  decision; the reader turns the column and the value into SQL.
- **The writer creates nothing on the target.** A missing target fails before any statement
  touches it (decision 15). The temporary stage the connector loads is the connector's own
  mechanism, not the target
  ([ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md)).
- **Essential by default.** The connectors catch nothing to carry on: every failure propagates out
  of `read()` or `write()`, and fails the callback that called them.
- **Simplicity.** Three small classes in one module, two rows of the table, one widened annotation
  per callback. No registry, no new protocol, no new callback, no Python client.
- **Compose, do not impose.** The job builds the `SnowflakeConnection` from its own secrets. The
  connectors read no file, environment variable or secret store. A job's own callback can still
  build a `SnowflakeReader` or a `SnowflakeWriter` itself.
- **No dependencies in the core.** No extra at all (decision 14): pyspark, the Snowflake Connector
  for Spark and the Snowflake JDBC driver are prerequisites that the runtime provides. `ingestio`
  and `ingestio.connector.all` still import without pyspark.
- **Streaming.** Nothing here closes the door; see [ADRs](#adrs).

## Scope

In scope:

- `SnowflakeConnection`: its checks, its two credentials, its `repr`, and its translation into the
  connector's options;
- `SnowflakeReader`: full reads, and watermark reads filtered inside Snowflake over a slice that
  `read()` freezes with an upper bound;
- `SnowflakeWriter`: atomic append (`write(data)`) and atomic replace (`write(data, replace=True)`),
  through the connector's transaction, with a missing target failing;
- the form of table and column names, and literals that keep any value text out of the SQL;
- the options the connectors set and reserve, and the session time zone;
- the `snowflake` system, as a source and as a target, its keys, and `connection` widened to
  `SnowflakeConnection` in the four callbacks, with its type check;
- `snowflake` as an `SCD2` target (decision 4);
- the test doubles, and the opt-in integration tests against a real Snowflake account.

Out of scope:

- password authentication, programmatic access tokens, encrypted private keys, OAuth client
  credentials, workload identity, browser and multi-factor sign-in (decision 3);
- a Python client on the driver, such as snowflake-connector-python or Snowpark (decision 2);
- upsert, merge, delete or any operation besides append and replace;
- creating, altering or dropping the target (any DDL on the target);
- custom SQL queries, column projection and extra filters in the reader;
- computing, persisting or advancing the watermark, and exposing the bound that `read()` captured;
- Snowflake streams, change tracking, Snowpipe and Structured Streaming;
- Lakehouse Federation, Databricks serverless compute, engines other than Spark;
- Iceberg, hybrid, external and dynamic tables, and views, as targets: no claim;
- retries, row counts, log lines;
- changes to CI, `pyproject.toml`, `uv.lock` and `README.md`, which belong to the maintainer and the
  main session (see [Where it lives](#where-it-lives)).

## Public API

New, in the new module `ingestio/connector/snowflake.py`, exported from `ingestio.connector.all`
only (not from `ingestio`) (decision 21):

```python
from ingestio.connector.all import SnowflakeConnection, SnowflakeReader, SnowflakeWriter
```

```python
@dataclass(frozen=True, repr=False)
class SnowflakeConnection:
    """Where and how to reach one Snowflake database. Opens nothing by itself.

    Args:
        host: The account's host name, such as `myorg-myaccount.snowflakecomputing.com`. The
            connector's `sfURL`.
        database: The session's database. A table name without a database resolves to it.
        user: The Snowflake user to sign in as. Required with `private_key`; optional with
            `oauth_token`, whose user the token names.
        private_key: An unencrypted PKCS#8 private key, as PEM text or its base64 body. Never
            shown in `repr`.
        oauth_token: An OAuth access token. Never shown in `repr`.
        schema: The session's schema. A table name without a schema resolves to it. The
            connector's default, `public`, when omitted.
        warehouse: The warehouse that runs the queries. The user's default when omitted.
        role: The session's role. The user's default when omitted.
    """

    host: str
    database: str
    _: KW_ONLY
    user: str | None = None
    private_key: str | None = None
    oauth_token: str | None = None
    schema: str | None = None
    warehouse: str | None = None
    role: str | None = None
```

```python
class SnowflakeReader:
    """Reads a Snowflake table into a Spark DataFrame, filtering inside Snowflake.

    Args:
        table_name: Source table, `table`, `schema.table` or `database.schema.table`, each part a
            Snowflake identifier as SQL writes it. The configuration key of the same name.
        connection: Where the table lives, with its credentials.
        watermark_column: Column that orders changes, a Snowflake identifier as SQL writes it.
            The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only rows above it are read.
            Requires `watermark_column`.
        options: Options of the Snowflake Connector for Spark, passed through, such as
            `autopushdown` or `sfTimezone`.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
        connection: SnowflakeConnection,
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
class SnowflakeWriter:
    """Writes a Spark DataFrame into a Snowflake table, atomically.

    Args:
        table_name: Table to write into, `table`, `schema.table` or `database.schema.table`, each
            part a Snowflake identifier as SQL writes it. It must exist.
        connection: Where the table lives, with its credentials.
        options: Options of the Snowflake Connector for Spark, passed through, such as
            `column_mismatch_behavior` or `sfCompress`.
    """

    def __init__(
        self,
        table_name: str,
        connection: SnowflakeConnection,
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
  `Decimal` is `decimal.Decimal`; `date` is `datetime.date`, and `datetime.datetime`, its subclass,
  is accepted too. `Mapping` is `collections.abc.Mapping`. `KW_ONLY` and `dataclass` are from
  `dataclasses`.
- `SnowflakeConnection` is immutable, and its arguments are readable as attributes of the same
  names, `private_key` as given. The attributes of `SnowflakeReader` and `SnowflakeWriter` are not
  documented, so they are not public API. Their `repr` is specified under
  [Behaviour](#snowflakeconnection).
- `host` and `database` are the positional arguments of `SnowflakeConnection`, in this order; every
  other argument is keyword-only (decisions 22, 23 and 24). `table_name` and `connection` are
  required on both connectors, so they are positional, in this order, as on the MySQL connectors.
- `SnowflakeWriter` has one write method, `write`, and no `replace()` method; `replace` is
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
        connection: (
            PostgresConnection | MongoDBConnection | MySQLConnection | SnowflakeConnection | None
        ) = None,
        client: object | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...


class AppendOnly(Callback):  # FullLoad and SCD2 take the same connection
    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        key: str = "records",
        connection: PostgresConnection | MySQLConnection | SnowflakeConnection | None = None,
        client: object | None = None,
    ) -> None: ...
```

**Changed: the configuration:**

- the system `snowflake`, a source that builds `SnowflakeReader` and a target that builds
  `SnowflakeWriter`;
- source keys: `table_name` (required), `watermark_column` and `options`; target keys:
  `table_name` (required) and `options`;
- callback arguments: `connection`, a `SnowflakeConnection`, required; `watermark_value` and `spark`
  optional for the source; `client` not taken;
- `snowflake` is a target of `SCD2` (decision 4);
- the lists of systems in the messages `<path>: <section>.system '<name>' is not one of: <names>`
  gain `snowflake`, and so does `SCD2`'s narrowed list.

**No new extra** (decision 14).

**Unchanged:** `Reader`, `Writer`, every other connector and connection value, and everything
exported by `ingestio` other than the annotations above.

### Usage sketch

A sketch of a Databricks job, not a runnable example: `dbutils` and `last_watermark()` are the
job's. The configuration, `orders.json`:

```json
{
  "source": {"system": "snowflake", "table_name": "shop.public.orders", "watermark_column": "updated_at"},
  "target": {"system": "unity_catalog", "table_name": "main.bronze.orders"}
}
```

```python
from ingestio import AppendOnly, Process, ReadSource, Step
from ingestio.connector.all import SnowflakeConnection

shop = SnowflakeConnection(
    "myorg-myaccount.snowflakecomputing.com",
    "shop",
    user="ingest",
    private_key=dbutils.secrets.get("snowflake", "private_key"),  # the job reads its secrets
    warehouse="ingest_wh",
)

Process([
    Step("extract", [ReadSource("orders.json", connection=shop, watermark_value=last_watermark())]),
    Step("load", [AppendOnly("orders.json")]),
]).run()
```

A full load into Snowflake, from a file whose `target` is
`{"system": "snowflake", "table_name": "reporting.public.daily_sales"}`, places
`FullLoad("daily_sales.json", connection=shop)` instead of `AppendOnly`. A job that signs in with
OAuth builds `SnowflakeConnection(host, "shop", oauth_token=<token>)`.

## Behaviour

### Evidence

The main session read the sources below and gave the planner the facts F1 to F13 before the first
draft, and V1 to V10 after it; the spec cites them by number. V1 to V9 replace the planner's K11,
K14, K16, K17, K19, K20 and K22, and settle the inference of F5 and the order of F6. The K facts
that remain are the planner's own knowledge, **not verified**: each is cited as "(K<n>, to be
confirmed)" where a rule rests on it, and is to be confirmed in the source or the documentation it
names, or by the [integration tests](#integration-tests).

**The Snowflake Connector for Spark**, github.com/snowflakedb/spark-snowflake, tag v3.2.3 (the
latest, 2026-10-22) unless stated; paths under `src/main/scala/net/snowflake/spark/snowflake/`:

- **F1** `Parameters.scala` L299-312, `DEFAULT_PARAMETERS`: `usestagingtable` = "true",
  `truncate_table` = "off", `preactions` = "", `postactions` = "", `sfssl` = "on",
  `keep_column_case` = "off", `column_mapping` = "order", `column_mismatch_behavior` = "error".
- **F2** `io/StageWriter.scala` L286-291, `writeToTable`: `if (params.useStagingTable ||
  !params.truncateTable)` goes to `writeToTableWithStagingTable`, else to
  `writeToTableWithoutStagingTable`. The transactional path needs `usestagingtable` off **and**
  `truncate_table` on. The same routing line and the `START TRANSACTION` statement exist at tags
  v2.11.0-spark_3.3, v2.12.0-spark_3.4, v2.16.0-spark_3.4, v3.0.0 and v3.1.0.
- **F3** `io/StageWriter.scala` L37-51 (comment), L92-98, L100-135, L298-354,
  `writeToTableWithoutStagingTable` and `WriteTableState`: it checks whether the table exists; if
  it does not, it runs `createTable` from the DataFrame's schema (DDL, before the transaction); if
  it exists and the mode is Overwrite with `truncate_table` on, it runs `TRUNCATE` inside a user
  transaction `START TRANSACTION NAME spark_connector_tx_<n>`; `copyIntoTable` begins the
  transaction if it has not begun, runs the `preactions` inside it, then `COPY INTO`; then
  `commit()`; on any `Throwable`, `rollback()`, which rolls back the `TRUNCATE` and the `COPY` and
  drops a table it created. The class comment: "Snowflake doesn't support DDL in tran, so the State
  Machine is created to maintain the ATOMIC of write."
- **F4** `io/StageWriter.scala` L375-490, `writeToTableWithStagingTable` (commented "This function
  is deprecated."): in Overwrite with `usestagingtable` on, it creates `<table>_staging_<random>`
  (`LIKE` the target only when `truncate_table` is on, otherwise from the DataFrame's schema),
  copies into it, then swaps it with the target (`swapTable`) and drops it; in Append, it copies
  into the target and calls `conn.commit()`, with no `START TRANSACTION`. The `COPY` may run twice,
  the second time with a `FILES` clause for files missed by eventual consistency (comment above
  `executeCopyIntoTable`, around L490-500).
- **F5** `SnowflakeWriter.scala` L67-76; `Parameters.scala` L951-968 (`setColumnMap`);
  `io/StageWriter.scala` L779-783 (`copySql`): with `column_mapping=name`, before writing, the
  connector resolves the target's schema from table metadata (`jdbcWrapper.resolveTable` →
  `conn.tableMetaData(table)`, `SnowflakeJDBCWrapper.scala` L75-78) and builds a map from the
  DataFrame's columns to the table's; with `column_mismatch_behavior=error` a mismatch raises; no
  name matching at all throws `UnsupportedOperationException`. Any column map in a save mode other
  than Append throws `UnsupportedOperationException("The column mapping only works in append
  mode.")`. The first draft's inference, that a missing target fails before any DDL, is settled by
  V1.
- **F6** `Utils.scala` L405-420 (`executePreActions`); `Parameters.scala` L1049: `preactions` is
  split on `;`, each non-empty action is executed, and `%s` in an action is replaced with the table
  name. The order and the name are V2.
- **F8** Authentication. `Parameters.scala` L340-415: `sfurl` is required; `sfuser` is required
  unless a token or OAuth client credentials are given; a token requires `sfauthenticator=oauth`
  (message around L394, "Please set 'sfauthenticator=oauth'."); one of `sfpassword`,
  `pem_private_key` or OAuth must be present. `ServerConnection.scala` L195-220: precedence OAuth
  client credentials > private key > `sftoken` > workload identity > password. `Parameters.scala`
  L1167-1190, `privateKey`: the value of `pem_private_key` goes through commons-codec
  `Base64.decodeBase64`, then a `PKCS8EncodedKeySpec` with an RSA `KeyFactory`; any failure throws
  `IllegalArgumentException("Input PEM private key is invalid")`. So it expects the base64 body of
  an unencrypted PKCS#8 RSA key. **The main session's inference:** the
  `-----BEGIN PRIVATE KEY-----` and `-----END PRIVATE KEY-----` lines must be stripped, because
  their letters and dashes belong to the standard and URL-safe base64 alphabets and would corrupt
  the decoded key.
- **F9** Time zone. `Utils.scala` L343-375 (`genPrologueSql`), `Parameters.scala` L998-1011: when
  `sftimezone` is unset it counts as `spark`, and the connector runs
  `alter session set timezone = '<java.util.TimeZone.getDefault.getID>'`, the JVM's default zone,
  not `spark.sql.session.timeZone`; `snowflake` leaves the session's zone; `sf_default` resets it;
  any other value is set as given. It also sets the timestamp output formats of the session, which
  V7 gives.
- **F10** Table names and queries. `TableName.scala`: "we don't do any escaping/unescaping for
  Snowflake tables, we expect the user to do it": `dbtable` is used verbatim. `Parameters.scala`
  L708-745: a `dbtable` wrapped in parentheses is treated as a query; a trailing `;` of `query` is
  removed; L429-433: `dbtable` and `query` are mutually exclusive. L773: `sfDatabase` is read from
  `sfdatabase`; L778: `sfSchema` defaults to `public`. Snowflake folds unquoted identifiers to upper
  case, so quoting a name makes it case-sensitive.
- **F11** Releases (GitHub releases of spark-snowflake): v3.2.0 (2026-06-18) requires Snowflake
  JDBC 4.0.2 or later, adds Spark 4.0 and 4.1, and a Scala 2.13 build for Spark 3.5; v3.2.1
  (2026-07-08) escapes embedded quotes when it quotes identifiers, and registers the sensitive
  options with Spark's log redaction; v3.2.3 (2026-10-22) fixes columns whose names contain double
  quotes; v3.1.9 adds OAuth client credentials; v3.1.7 adds workload identity. The 2.x line has one
  tag per Spark minor version (spark_3.2 to spark_3.4); the 3.x line drops the suffix. The
  project's development dependency is `pyspark>=3.5`.
- **F12** Reading, **not read in depth**: the connector takes `dbtable` or `query`, pushes filters
  and projections down when `autopushdown` is on, and runs a read as an unload (`COPY INTO @stage`)
  or an Arrow result. A frozen upper bound needs a bound query, as `MySQLReader` runs one through a
  `query` load.

**Snowflake** (the documentation, not source code):

- **F7** "Transactions" (docs.snowflake.com/en/sql-reference/transactions): "The term DML refers to
  INSERT, UPDATE, DELETE, MERGE, and TRUNCATE statements."; "DDL statements implicitly commit
  active transactions"; "Statements inside an explicit transaction are not affected by
  AUTOCOMMIT."; "When INSERT or COPY statements are executed on an object in an explicit
  transaction, avoid DDL statements on the same object in different sessions for the duration of
  the transaction." The "TRUNCATE TABLE" page: truncating also deletes the table's load metadata,
  which cannot be recovered; the deleted rows stay available through Time Travel for the retention
  period.
- **F13** "Planning for the deprecation of single-factor password sign-ins"
  (docs.snowflake.com/en/user-guide/security-mfa-rollout), revised October 2025: the final phase,
  strong authentication for all users, runs August to October 2026; `LEGACY_SERVICE` users are
  covered by the later phases. Snowflake's engineering blog states that it deprecates single-factor
  password authentication and the `LEGACY_SERVICE` user type.

**Read after the first draft**, in spark-snowflake at tag v3.2.3 unless stated, paths as above:

- **V1** (settles F5's inference and K16). `SnowflakeWriter.scala` L67-76: with
  `column_mapping=name` and no `columnmap`, `save()` first opens a connection and runs
  `jdbcWrapper.resolveTable(conn, table, params)`; `ServerConnection.scala` L480-489,
  `tableMetaData`, prepares `select * from <dbtable> where 1 = 0` and reads its metadata. This
  happens at the start of `save()`, before `io.writeRDD` (L95-105), which uploads the files and
  reaches `writeToTable` and its `createTable` (F3). So a missing target fails while that `select`
  is prepared, before any upload and any DDL. `Utils.scala` L581-650, `generateColumnMap`: names are
  matched by `toLowerCase` on both sides, ignoring case; two columns of the DataFrame, or of the
  table, that differ only in case and are both matched throw
  `UnsupportedOperationException("Duplicated column names ...")`. With
  `column_mismatch_behavior=error` (the default, F1), the DataFrame's column count must equal the
  table's (`UnsupportedOperationException("column number of Spark Dataframe (n) doesn't match column number of Snowflake Table (m)")`)
  and every DataFrame column must exist in the table (`"can't find column <name> in Snowflake Table"`):
  a write must carry exactly the target's columns, unlike `MySQLWriter`, which accepts a subset.
  With `ignore`, unmatched columns are left out. `SnowflakeWriter.scala` L249-269,
  `removeUselessColumns`: the DataFrame is reduced to the mapped columns before the write.
- **V2** (settles the order of F6). `Parameters.scala` L1049:
  `preActions = parameters("preactions").split(";")`; `Utils.scala` L405-420: each non-empty action
  that contains `%s` becomes `action.format(table.get)`, where `table.get` is the `TableName` of
  `dbtable`, whose `toString` is `dbtable` as given (`TableName.scala`). The split happens before
  the substitution, so a `;` inside the table name does not split the action.
  `io/StageWriter.scala` L113-135: `copyIntoTable` calls `beginTranIfNotBeginYet()` before
  `executePreActions`, so the action runs inside the transaction, before the `COPY`.
- **V3** (settles K11). `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`
  lists `net.snowflake.spark.snowflake.DefaultSource`, and
  `src/main/3.5/scala/.../DefaultSource.scala` L39, `shortName()`, returns
  `SNOWFLAKE_SOURCE_SHORT_NAME`. The constant's value, `snowflake`, is the documented short name;
  the file that defines it was not opened. `DefaultSource.scala` L87-89: Append gives
  `(doSave = true, dropExisting = false)`, with no existence check in this method.
- **V4** (settles K14 but for its residual, and question 13). `io/StageWriter.scala` L195-260,
  `writeToStage`: the session prologue (F9) runs, then a temporary stage is created and the files
  are uploaded, all before `writeToTable`. When no file was uploaded (an empty DataFrame) and the
  internal option `internal_skip_write_when_writing_empty_dataframe` is false (its default,
  `Parameters.scala` L203-205 and L925-927), the connector still calls `writeToTable` with a dummy
  prefix `dummy_not_exist_prefix_<n>`, so the transaction, the `TRUNCATE` preaction and the `COPY`
  still run. When that option is true, `writeToTable` is skipped. **Not read:** that a `COPY` over a
  prefix with no files succeeds with zero rows; the opt-in integration tier checks it.
- **V5** (settles K17 and K19). `Parameters.scala` L333-337, `mergeParameters`: every option key is
  lowercased, so option names are case-insensitive. L274-277: a boolean is true for `on`, `yes`,
  `true`, `1`, `enabled` and false for `off`, `no`, `false`, `0`, `disabled`, in any case. Option
  names: `sfaccount` (L50), `sfauthenticator` (L104), `sftoken` (L105),
  `sfworkloadidentityprovider` (L106), `oauthclientid`, `oauthclientsecret`,
  `oauthtokenrequesturl`, `oauthscope` (L110-113), `connection_id` (L259); the authenticator value
  of OAuth client credentials is `oauth_client_credentials` (L271). L339-340: when
  `connection_id` is given, `mergeParameters` skips the URL and authentication checks.
- **V6** (settles K20). `Parameters.scala` L773:
  `def sfDatabase: String = parameters(PARAM_SF_DATABASE)`, a lookup that throws when the key is
  missing; `ServerConnection.scala` L195 always calls it when it builds the JDBC properties. So the
  connector requires `sfDatabase` on every connection.
- **V7** (completes F9). `Parameters.scala` L320-322, defaults: `timestamp_ntz_output_format` =
  `YYYY-MM-DD HH24:MI:SS.FF3`, `timestamp_ltz_output_format` and `timestamp_tz_output_format` =
  `TZHTZM YYYY-MM-DD HH24:MI:SS.FF3`. `Utils.scala` L343-375: the prologue sets these three session
  output formats (unless a value is `sf_current`) together with the time zone, on reads and on
  writes (`writeToStage` runs it, V4). So a timestamp turned into text without an explicit format,
  in the connector's session, carries three fractional digits and, for `TIMESTAMP_NTZ`, no offset.
- **V8** (the minimum version). GitHub compare v3.2.1...v3.2.3: outside the tests and the fallback
  catalog, the only changes are `build.sbt`, `FilterPushdown.scala` (+4 -1),
  `SnowflakeJDBCWrapper.scala` (one line: a column name is quoted with its embedded `"` doubled) and
  `Utils.scala` (`isPlainIdentifier`, and `removeQuote` unescaping `""` inside a quoted name).
  `StageWriter.scala`, `SnowflakeWriter.scala`, `Parameters.scala`, `ServerConnection.scala` and
  `DefaultSource.scala` are unchanged. So F2, F3, F5, F6, F8, V1, V2, V4, V5, V6 and V7 hold from
  3.2.1; only columns whose names contain `"` behave differently before 3.2.3 (F11).
- **V9** (settles K22 as far as read). The Databricks release notes of Databricks Runtime 17.3 LTS
  (Spark 4.0.0) and Databricks Runtime 18 (Spark 4.1.0) list, among the installed Java and Scala
  libraries, `net.snowflake snowflake-ingest-sdk 0.9.6`, and no `spark-snowflake` or
  `snowflake-jdbc` row (docs.databricks.com/aws/en/release-notes/runtime/17.3lts and
  .../runtime/18). So on those runtimes the job installs the connector and the JDBC driver as
  cluster libraries. Other runtimes were not read.
- **V10** (settles question 14). `SnowflakeWriter.scala` L67-95 (v3.2.3): the metadata lookup of V1
  runs in the branch `if (params.columnMap.isEmpty && params.columnMapping == "name")`. When the
  user gives `columnmap`, that branch is skipped; the
  `else if (params.columnMap.isDefined && format == SupportedFormat.PARQUET)` branch resolves the
  table only for the Parquet format, and the default format is CSV (L56-65: Parquet only with
  `use_parquet_in_write` or a `VARIANT` column). So with a user's `columnmap` and the CSV format,
  nothing looks up the target before `io.writeRDD`, and the transactional path creates a missing
  target from the DataFrame's schema (F3), against ADR 0014. `Parameters.scala` L951-955:
  `setColumnMap` also throws `Exception("Column map is already declared")` when `columnmap` is set,
  but it is only called from the skipped branch.

**The planner's knowledge, to be confirmed.** Snowflake's documentation:

- **K1** ("Identifier requirements"): an unquoted identifier starts with a letter (A to Z, a to z)
  or `_`, and holds only letters, digits, `_` and `$`; Snowflake stores and resolves it in upper
  case. A double-quoted identifier keeps its case and may hold any character, a `"` inside it
  written `""`; an identifier is never empty. A reserved word must be quoted. A name with fewer
  than three parts resolves against the session's current database and schema.
- **K2** ("String and binary data types", string constants): in a single-quoted string constant,
  `\` starts an escape sequence (`\\` is a backslash) and `''` is a quote; no session parameter turns
  the escapes off.
- **K3** (date and time input formats): with the default `DATE_INPUT_FORMAT` and
  `TIMESTAMP_INPUT_FORMAT`, `AUTO`, a string compared with a `DATE` or `TIMESTAMP_*` column is
  converted to the column's type; ISO 8601 text with up to nine fractional digits and an offset is
  recognised; for `TIMESTAMP_LTZ` and `TIMESTAMP_TZ` the offset fixes the instant; for
  `TIMESTAMP_NTZ` the offset is ignored and the wall clock kept.
- **K4** (`TO_VARCHAR` and its format elements): `FF9` writes nine fractional digits; `TZH:TZM`
  writes the value's offset, for a `TIMESTAMP_LTZ` the session zone's offset at that instant. What
  `TZH:TZM` writes for a `TIMESTAMP_NTZ` is not established.
- **K5**: an unquoted numeric literal compared with a `NUMBER` column compares exactly.
  `TO_VARCHAR` of a `NUMBER` without a format is its exact decimal text; that the text of a `FLOAT`
  converts back to the same value is not established.
- **K6**: a column of the target that a `COPY INTO <table> (<columns>)` leaves out takes its default,
  or `NULL`.
- **K7**: Snowflake runs statements at `READ COMMITTED`: other sessions see the rows of a
  transaction only after it commits, and the old rows of a table truncated inside it until then.
- **K8**: a `TIMESTAMP_*` column holds up to nanoseconds.
- **K9**: an account's host name is not always `<orgname>-<account_name>.snowflakecomputing.com`:
  the legacy account locator with its region, private connectivity (`privatelink`) and other
  domains give other host names, which the connector takes as `sfURL`.
- **K10**: the role needs `USAGE` on the warehouse, the database and the schema; `SELECT` on a
  source table; `INSERT` on a target and, for a replace, what `TRUNCATE TABLE` requires (the
  table's ownership, or a privilege of its own); and `CREATE STAGE` on the session's schema, for the
  connector's temporary stage (K15).

The Snowflake Connector for Spark:

- **K12**: it maps `NUMBER` to `DecimalType` (or an integral type for some scales), `FLOAT` to
  `DoubleType`, `VARCHAR` to `StringType`, `DATE` to `DateType` and every `TIMESTAMP_*` to
  `TimestampType`; whether a release maps `TIMESTAMP_NTZ` to `TimestampNTZType` is not established.
- **K13**: it resolves the schema of a load with a query that returns no row, such as
  `SELECT * FROM (<query>) WHERE 1 = 0`, with no unload.
- **K14** (the residual that V4 leaves): that the upload evaluates the DataFrame exactly once is not
  established.
- **K15**: the temporary stage lives in the session's current database and schema, for the length
  of the session.
- **K18**: whether a read runs `preactions` and `postactions` is not established.
- **K21**: `net.snowflake.spark.snowflake.Utils.runQuery(<options>, <statement>)` runs one statement
  from the driver and can be called through py4j.

K11, K16, K17, K19, K20 and K22 are replaced by V3, V1, V5, V5, V6 and V9, and K14 by V4 but for the
residual above.

### `SnowflakeConnection`

`SnowflakeConnection` validates its arguments when it is built, in this order; the first that
fails raises `ValueError` (decisions 23 to 25). An argument is *given* when it is not `None`.

| Argument | Rule | Message |
|---|---|---|
| `host` | not empty | `host must not be empty` |
| `database` | not empty | `database must not be empty` |
| `user` | `None` or not empty | `user must not be empty` |
| `schema` | `None` or not empty | `schema must not be empty` |
| `warehouse` | `None` or not empty | `warehouse must not be empty` |
| `role` | `None` or not empty | `role must not be empty` |
| `private_key`, `oauth_token` | at least one given | `give private_key or oauth_token` |
| `private_key`, `oauth_token` | not both given | `give private_key or oauth_token, not both` |
| `oauth_token` | not empty | `oauth_token must not be empty` |
| `private_key` | `user` given | `private_key needs user` |
| `private_key` | the form below | the messages below |

**The private key** (decision 25). The text, with its leading and trailing whitespace removed, is
read in one of two forms:

- **PEM.** It starts with `-----BEGIN `. Its lines, as `str.splitlines()` splits them, so that
  `\r\n` ends a line too: the first must be exactly `-----BEGIN PRIVATE KEY-----` and the last
  exactly `-----END PRIVATE KEY-----`; the *body* is the lines between them.
- **Body.** Otherwise the whole text is the body.

Then the body, with every whitespace character removed, must be non-empty and decode with
`base64.b64decode(body, validate=True)`. Failures, checked in this order:

| Case | Message |
|---|---|
| the first line is `-----BEGIN ENCRYPTED PRIVATE KEY-----` | `private_key must be unencrypted` |
| the first line is `-----BEGIN RSA PRIVATE KEY-----` | `private_key must be PKCS#8 (BEGIN PRIVATE KEY), not PKCS#1 (BEGIN RSA PRIVATE KEY)` |
| any other PEM first line, a missing or different last line, an empty body, a body that does not decode | `private_key must be an unencrypted PKCS#8 key, as PEM text or its base64 body` |

- The connector receives the body without whitespace as `pem_private_key`, never the PEM lines
  (F8, and its inference). Whether the key is an RSA key is not checked: the connector raises
  `Input PEM private key is invalid` when it connects (F8).
- No message holds the key or any part of it. The attribute `private_key` keeps the text as given.
- Building imports nothing outside the standard library and opens nothing.

**The connection options**, which every load and save carries (decisions 22 to 24):

| Option | Value | Sent |
|---|---|---|
| `sfURL` | `host` | always |
| `sfDatabase` | `database` | always |
| `sfSchema` | `schema` | when given |
| `sfWarehouse` | `warehouse` | when given |
| `sfRole` | `role` | when given |
| `sfUser` | `user` | when given |
| `pem_private_key` | the key's body | with `private_key` |
| `sfAuthenticator` | `oauth` | with `oauth_token` |
| `sfToken` | `oauth_token` | with `oauth_token` |

The values are sent as given. Snowflake reads the database, the schema, the warehouse and the role
as identifiers (K1): a name written unquoted is folded to upper case.

**`repr`** shows what identifies the database or the table, and never the key or the token; `str`
is the same:

- `SnowflakeConnection(host='acme-shop.snowflakecomputing.com', database='shop', schema=None, warehouse=None, role=None, user='ingest')`
- `SnowflakeReader(table_name='shop.public.orders', host='acme-shop.snowflakecomputing.com', database='shop')`
- `SnowflakeWriter(table_name='shop.public.orders', host='acme-shop.snowflakecomputing.com', database='shop')`

### Table and column names

(Decisions 26 and 27.)

- **`table_name`**, on both connectors, has one, two or three parts separated by `.`: `table`,
  `schema.table` or `database.schema.table`. A part left out resolves to the session's database and
  schema, the connection's (K1).
- **Each part is a Snowflake identifier as SQL writes it** (K1): either *unquoted*, matching
  `[A-Za-z_][A-Za-z0-9_$]*`, or *quoted*, matching `"([^"]|"")+"`. A `.` inside a quoted part
  belongs to the name. Anything else raises
  `ValueError("table_name must be 'table', 'schema.table' or 'database.schema.table', each part a Snowflake identifier")`
  when the connector is built.
- **`watermark_column`** is one identifier in the same two forms. An empty one raises
  `ValueError("watermark_column must not be empty")`; any other text that is not an identifier
  raises `ValueError("watermark_column must be a Snowflake identifier")`.
- **The connectors write every name as given**: never quoted, unquoted or changed in case by the
  connector. Snowflake folds an unquoted part to upper case and keeps a quoted one as written (K1),
  so `shop.public.orders` names `SHOP.PUBLIC.ORDERS`, and `"Shop".public."Orders"` names a database
  and a table created with quoted mixed-case names. Neither form can hold a character that ends the
  identifier outside its quotes, so no other text reaches the SQL through a name.
- `dbtable` receives `table_name` as given; the connector does no escaping of its own (F10). A text
  in parentheses, which the connector would read as a query (F10), is not an identifier and is
  rejected.
- An unquoted reserved word fails in Snowflake (K1): a column named `order` is written `"ORDER"`.
- The writer writes no column name itself: the connector maps the DataFrame's columns to the
  target's by name (decision 15, V1).
- A read returns the columns as Snowflake names them: an unquoted column comes back in upper case,
  such as `UPDATED_AT` (K1). Spark resolves column names ignoring case unless the session sets
  `spark.sql.caseSensitive`.

### Reading

`read()` returns a `pyspark.sql.DataFrame` built with `spark.read.format("snowflake")`. The rows
move when an action runs on it. Apart from the connector's own work, `read()` runs at most two
loads before the one whose DataFrame it returns: the type load and the bound load described below.

**Spark session.** The reader uses `spark` when it is given. Otherwise it calls
`pyspark.sql.SparkSession.active()`, when `read()` is called, never when the reader is built. The
reader never creates a session; when none is active, PySpark's error propagates.

**Options of every load**, the connection options, then `dbtable` or `query` as each load says, then
the user's `options`. No user option can replace one of the connector's, because those are reserved
([Options](#options)).

**Full read** (no `watermark_column`): one load, with `dbtable` = `table_name`, as given.

**Watermark read** (`watermark_column` given; decision 28):

1. **Type load.** A load with `query` =

   ```text
   SELECT <col> FROM <table> WHERE FALSE
   ```

   The reader reads the `schema` of its DataFrame once, and runs no action on it. The data type of
   the schema's first field, by position, is the *watermark type*. The query returns no row, and
   the connector resolves its schema without an unload (K13, to be confirmed).
2. **Bound load.** A load with `query` =

   ```text
   SELECT <render> AS UPPER_BOUND FROM <table>
   ```

   followed by ` WHERE <col> > <value>` when a value is given, where `<render>` depends on the
   watermark type (an instance of the `pyspark.sql.types` class named):

   | Watermark type | `<render>` |
   |---|---|
   | `DateType` | `TO_VARCHAR(MAX(<col>), 'YYYY-MM-DD')` |
   | `TimestampType` or `TimestampNTZType` | `TO_VARCHAR(MAX(<col>), 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')` |
   | any other | `TO_VARCHAR(MAX(<col>))` |

   The reader calls `first()` on its DataFrame once. The *bound* is the row's first field, by
   position: the highest watermark of the slice as Snowflake's text, or `None` when `first()`
   returns `None` or the field is `None`, that is when no row qualifies.
3. **Data load.** A load with `query` = `SELECT * FROM <table> WHERE <predicate>`, where:

   | Value given | Bound | `<predicate>` |
   |---|---|---|
   | yes | not `None` | `<col> > <value> AND <col> <= <bound>` |
   | no | not `None` | `<col> <= <bound>` |
   | either | `None` | `FALSE` |

4. `read()` returns the data load's DataFrame unchanged. The reader adds no Spark-side filter.

`<table>` is `table_name` and `<col>` is `watermark_column`, as given. `<value>` and `<bound>` are
literals, built as [Literals](#literals) says. Every query uses single spaces, exactly as shown.

What this gives, as for `MySQLReader`:

- The filter is in the SQL that Snowflake runs, so it does not depend on the connector's
  `autopushdown` (F12).
- Every action on the DataFrame reads the same range of the watermark. Rows written after `read()`
  with a watermark above the bound are left for the next run, so a watermark that the job computes
  from the DataFrame never goes past what the DataFrame holds.
- Rows whose watermark is `NULL` are never read when `watermark_column` is given, first run
  included: `NULL` satisfies neither comparison.
- The reader imports `pyspark.sql.types` only in a watermark read, to test the watermark type.

### Literals

(Decision 20.) **`Q(t)`**, for a text `t`, is `'`, then `t` with every `\` written `\\` and then
every `'` written `''`, then `'` (K2). For example `Q("O'Brien")` is `'O''Brien'`,
`Q(r"C:\temp")` is `'C:\\temp'`, and `Q("")` is `''`.

**Watermark values.** `watermark_value` is "given" when it is not `None`; `0` and `""` are values.
The literal of a value is built when the reader is built:

| Type | Literal |
|---|---|
| `int`, but not `bool` | `str(value)`, unquoted: `41`, `-7` |
| `decimal.Decimal`, finite | `format(value, "f")`, unquoted: `12.50`, `1000` |
| `str` | `Q(value)` |
| `datetime.datetime`, timezone-aware | `Q(value.astimezone(UTC).isoformat())`: `'2026-10-01T11:30:00.123456+00:00'` |
| `datetime.date` | `Q(value.isoformat())`: `'2026-10-01'` |

- **Which value for which column.** The value has the column's kind: an `int` for an integer
  `NUMBER`, a `Decimal` for a `NUMBER` with a scale, a `str` for a text column, a `date` for a `DATE`
  column, an aware `datetime` for a `TIMESTAMP_*` column. Snowflake converts a string compared with a
  `DATE` or `TIMESTAMP_*` column to the column's type (K3, to be confirmed).
- **Datetimes.** An aware datetime is converted to UTC and written with its offset, `+00:00`. A
  `TIMESTAMP_LTZ` or `TIMESTAMP_TZ` column compares it as the same instant, whatever the session's
  zone; a `TIMESTAMP_NTZ` column ignores the offset and compares it on the UTC wall clock (K3), as
  `PostgresReader` compares a `timestamp without time zone` column. A `TIMESTAMP_NTZ` column that
  holds wall-clock time in another zone takes its value as a `str`, which is sent as it is.

**The bound.** The bound is Snowflake's own text of the highest watermark, never converted. Its
literal is:

- the text as it is, unquoted, when the watermark type is a numeric type of Spark (an instance of
  `pyspark.sql.types.NumericType`) **and** the text, as a whole, matches the regular expression
  `[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?`;
- `Q(<text>)` otherwise.

So a `NUMBER` column is compared with a numeric literal, exactly (K5), a `DATE` with its ISO text,
and a `TIMESTAMP_*` with its ISO text to the nanosecond and its offset (K4), whatever the session's
output formats, which the connector sets to three fractional digits, with no offset for
`TIMESTAMP_NTZ` (V7). A numeric type whose text is not a number gets `Q` too, so no text that
Snowflake returns reaches the SQL unquoted unless it is a number.

The reader rejects, when it is built:

| Case | `ValueError` message |
|---|---|
| `watermark_column == ""` | `watermark_column must not be empty` |
| `watermark_column` not an identifier | `watermark_column must be a Snowflake identifier` |
| `watermark_value` given without `watermark_column` | `watermark_value needs watermark_column` |
| `bool`, `float`, or a type not in the table above | `unsupported watermark_value type: <type name>` |
| a `Decimal` that is NaN or infinite | `watermark_value must be finite` |
| a naive `datetime` (its `tzinfo` or its `utcoffset()` is `None`) | `watermark_value must be timezone-aware` |

`<type name>` is `type(value).__name__`. The checks run in the order of the table, after the check
of `table_name` and before the check of `options`, as `MySQLReader` runs its own.

### Time zones

(Decision 29.) The connectors set no `sfTimezone`. The connector then sets the session's zone to the
JVM's default zone before each load and save (F9), and a user's `sfTimezone` passes through. What
this gives:

- the value's literal carries its offset and the bound carries its own (K3, K4), so neither depends
  on the session's zone, and a repeated hour in that zone makes no literal ambiguous;
- the values the connector moves in and out of Spark follow the connector's own handling of the
  session's zone (F9), which the connectors do not change: the reader and the writer of an `SCD2`
  rewrite run with the same setting.

A user's `sfTimezone` changes the session's zone (F9); what that does to the values the connector
moves is the connector's, and is not established here.

### Options

(Decision 30.)

- **Reserved keys.** Giving one of them in `options`, in any case, raises
  `ValueError("option '<key>' is set by the connector")` when the connector is built, with `<key>`
  as given. The connector reads every option name in lower case (V5), so names are compared
  ignoring case:

  | Reserved by | Keys | Why |
  |---|---|---|
  | both | `sfURL`, `sfDatabase`, `sfSchema`, `sfWarehouse`, `sfRole`, `sfUser`, `sfPassword`, `pem_private_key`, `sfAuthenticator`, `sfToken`, `sfaccount`, `sfworkloadidentityprovider`, `oauthclientid`, `oauthclientsecret`, `oauthtokenrequesturl`, `oauthscope`, `connection_id` | they say where the session goes and who signs in, which the connection holds; a password, OAuth client credentials or workload identity in `options` would replace or outrank the connection's credential (F8, V5), and `connection_id` skips the connector's checks of the URL and the authentication (V5) |
  | both | `dbtable`, `query` | set by the connector from `table_name` and the watermark (F10) |
  | writer | `usestagingtable`, `truncate_table` | they route the connector to its transaction (F2) |
  | writer | `column_mapping`, `columnmap` | `column_mapping` makes a missing target fail and maps columns by name (decision 15, V1); a `columnmap` skips the lookup that makes a missing target fail (decision 34, V10) |
  | writer | `preactions`, `postactions` | the writer sets `preactions` for a replace; a DDL statement in an action commits the transaction (F7), and where `postactions` run relative to it is not established |
  | writer | `internal_skip_write_when_writing_empty_dataframe` | when true, the connector skips its transaction for an empty DataFrame, so a replace would leave the target unchanged (V4, decision 33) |

- **Every other key passes through**, in particular `autopushdown`, `sfTimezone`, `sfCompress`,
  `parallelism`, `keep_column_case`, `column_mismatch_behavior` and `continue_on_error`. On the
  reader, the writer's reserved keys pass through too: `preactions` and `postactions` reach the
  connector as given (K18).
- Values are passed to Spark as given. Spark turns them into strings, and booleans into `true` and
  `false`.
- `column_mismatch_behavior=ignore` leaves out the columns that do not match, the DataFrame's that
  the target lacks and the target's that the DataFrame lacks (V1), and `continue_on_error=on` lets
  the `COPY` skip rows it cannot load, both committed with the rest: a job that passes them chooses
  that (see [Known limitations](#known-limitations)).

### Writing

**Operations.**

- `write(data)`, that is with `replace=False`, appends: it inserts every row of `data` into the
  target and leaves the existing rows untouched. `AppendOnly` calls it.
- `write(data, replace=True)` deletes every row of the target and then inserts every row of `data`,
  in the same transaction (decision 2). The target ends up holding exactly the rows of `data`.
  `FullLoad` and `SCD2` call it.
- The calling callback chooses, at call time. The connector's configuration has no mode.

**The save** (decision 15):

0. When `data.columns` is empty, the writer raises `ValueError("the DataFrame has no columns")` and
   does nothing else.
1. `data.write.format("snowflake").options(...).mode("append").save()`, once, with these options:
   the connection options; `dbtable` = `table_name`, as given; `usestagingtable` = `off`;
   `truncate_table` = `on`; `column_mapping` = `name`; with `replace=True` only, `preactions` =
   `TRUNCATE TABLE %s`; and the user's `options`. The mode is `append` for both operations.

**What the connector then does**, as F2, F3, F5, F6, V1, V2 and V4 read:

1. **Columns and the target.** With `column_mapping=name`, at the start of `save()`, it prepares
   `select * from <dbtable> where 1 = 0` to read the target's columns, and maps the DataFrame's
   columns to them by name, ignoring case (V1). A missing target raises here, before any upload and
   any DDL (V1). With `column_mismatch_behavior=error`, the default (F1), the DataFrame must hold
   exactly the target's columns: a different number of columns, or a DataFrame column that the
   target lacks, raises `UnsupportedOperationException` (V1); two columns that differ only in case
   raise too (V1).
2. **Stage.** The session's prologue runs, a temporary stage is created and the DataFrame's files
   are uploaded to it, all before any statement on the target (V4).
3. **Transaction.** `usestagingtable` off and `truncate_table` on route it to its transactional
   path (F2). The table exists, so it creates nothing (F3). It begins
   `START TRANSACTION NAME spark_connector_tx_<n>`, runs the `preactions` inside it (V2), on a
   replace `TRUNCATE TABLE <table_name>` (the connector puts `dbtable`, as given, in place of `%s`,
   after splitting the actions on `;`, V2), then `COPY INTO <table_name> (<columns>)` from the stage
   (F5), then commits. On any failure it rolls back, the `TRUNCATE` included (F3). With an empty
   DataFrame, no file is uploaded, and the transaction, the `TRUNCATE` and the `COPY` still run
   (V4, decision 33).

**Atomicity.** `TRUNCATE` is DML in Snowflake (F7), so the truncation and the load commit together,
or not at all; other sessions see none of the new rows until the commit, and with `replace=True`
keep seeing the old rows until then (K7, to be confirmed). After a failure at any step, the target
is as it was.

**Columns.** The DataFrame's columns are matched to the target's by name, in any order, ignoring
case (V1). With the default `column_mismatch_behavior`, a write carries exactly the target's
columns, unlike `MySQLWriter`, which accepts a subset (V1). With `column_mismatch_behavior=ignore`,
a column of the target that the DataFrame lacks is left out of the `COPY` (V1) and takes its
default, or `NULL` (K6, to be confirmed).

**Failures.** The writer catches nothing.

| Fails | Then |
|---|---|
| step 0 | `ValueError`, and no save |
| the save, at any point: a missing target, columns that do not match the target's (V1), a failed stage upload, a failed `TRUNCATE` or `COPY`, a failed commit | the exception propagates unchanged, the same object; the connector rolled back whatever its transaction had done (F3) |

There is nothing for the writer to clean up: it creates nothing, and the temporary stage belongs to
the connector's session (K15).

**What the writer does not do.**

- It runs no DDL on the target and never creates it. A missing target fails before any statement
  touches it (decision 15). If the target is dropped by another session between the connector's
  metadata lookup and its own existence check, the connector creates it (F3): see
  [Known limitations](#known-limitations).
- It reads only `data.columns` and `data.write` from the DataFrame, and calls `save()` once. It
  never counts or inspects rows. An empty DataFrame goes through the same save: an append inserts
  nothing, and with `replace=True` the connector still runs its transaction and the `TRUNCATE` (V4,
  decision 33), so the target ends up empty, provided that the `COPY` of no file succeeds, which is
  not read and which the integration tier checks.
- It looks up no Spark session and imports nothing. `write` returns `None`.

### In a configuration

The `snowflake` system is two more rows of the closed table:

| `system` | `source` builds | `target` builds | Extra the connector may need |
|---|---|---|---|
| `snowflake` | `SnowflakeReader` | `SnowflakeWriter` | none |

| `system` | Section | Required keys | Optional keys |
|---|---|---|---|
| `snowflake` | source | `table_name` | `watermark_column`, `options` |
| `snowflake` | target | `table_name` | `options` |

| `system` | Section | `connection` | `client` | `watermark_value` | `spark` |
|---|---|---|---|---|---|
| `snowflake` | source | **required**: a `SnowflakeConnection` | no | optional | optional |
| `snowflake` | target | **required**: a `SnowflakeConnection` | no | – | – |

- **Keys.** Each key is the constructor argument of the same name, passed as it is, with the JSON
  types the other systems use for them: `table_name` and `watermark_column` strings, `options` an
  object of strings, numbers and booleans. The host, the database, the user, the key, the token, the
  schema, the warehouse and the role are never keys: like any key the system does not take, they
  raise `<path>: <section>.<key> is not a key of system 'snowflake'`. `schema` in particular is a key
  of the file systems, a DDL string, and not one of `snowflake`.
- **Lists of systems.** `<names>` for a source becomes
  `bigquery, disk, gcs, json_api, mongodb, mysql, postgres, s3, snowflake, unity_catalog, volume`,
  and for a target `bigquery, disk, gcs, json_lines, mysql, postgres, s3, snowflake, unity_catalog, volume`.
  These are the lists of the code this spec starts from. If the `mysql_cdc` source of
  `specs/mysql_cdc.md` lands first, the source list also holds `mysql_cdc`, after `mysql`, and the
  criteria that quote the list follow it.
- **`connection`.** The callback hands the very object to the connector. When it is missing, the
  build raises `<path>: <section> system 'snowflake' needs the argument connection`.
- **The type of `connection`.** The callback checks that it is an instance of `SnowflakeConnection`,
  a subclass included, after the other checks of the callback arguments and before it builds the
  connector; otherwise it raises
  `ValueError("<path>: <section> system 'snowflake' needs a SnowflakeConnection as the argument connection, not <type name>")`.
  The checks of `postgres`, `mongodb` and `mysql` are unchanged: a `SnowflakeConnection` given to
  them raises their message.
- **Errors of the connector.** A `ValueError` that the connector raises when the callback builds it
  propagates as the same object, with the note
  `while building the <section> of <path>, system 'snowflake'`, as for every system.
- Building a callback builds the connector and nothing else: no Spark session, no network.

### SCD2

(Decision 4.) `snowflake` joins the targets that `SCD2` supports, which become `bigquery`, `mysql`,
`postgres`, `snowflake` and `unity_catalog`. Decision 26 of
[specs/scd2_callback.md](scd2_callback.md) admits the systems whose `write(data, replace=True)` is
atomic and stays correct when the DataFrame written is a lazy plan that reads the same target. For
`snowflake`:

| Full read of the target | `write(data, replace=True)` | Atomic | `data` reads the target |
|---|---|---|---|
| one `dbtable` load of the table | the connector stages `data`, then one transaction runs `TRUNCATE` and `COPY INTO` | yes: one transaction, rolled back on any failure (F3, F7) | `data` is uploaded whole, into the stage, before the transaction touches the target (V4) |

- The target's reader is `SnowflakeReader`, built when the process runs with the `target` section's
  `table_name`, the `connection` that the writer received, and `spark=state[key].sparkSession`, as
  decision 25 of that spec describes; the section's `options` go to the writer only.
- `SCD2` compares column names ignoring case, so a target whose unquoted columns come back in upper
  case (`VALID_FROM`) matches the default names `valid_from` and `valid_to`; the writer maps the
  rewritten table's columns by name (decision 15).
- The narrowed list of the message becomes
  `<path>: target.system '<name>' is not one of: bigquery, mysql, postgres, snowflake, unity_catalog`.
- Each run rewrites the whole dimension, which on `snowflake` means a `TRUNCATE` and a `COPY` of
  every row in one transaction, with the costs of [Known limitations](#known-limitations).

### Where it lives

- `src/ingestio/connector/snowflake.py`: `SnowflakeConnection`, `SnowflakeReader` and
  `SnowflakeWriter`, with a module docstring and a sorted `__all__`. pyspark is imported lazily, and
  for annotations under `TYPE_CHECKING`.
- `src/ingestio/connector/all.py`: exports the three classes.
- `src/ingestio/callback/_configuration.py`: the `snowflake` rows of the source and target tables,
  with `SnowflakeConnection` as their connection type, and `snowflake` among `SCD2`'s systems.
- `src/ingestio/callback/io.py`: the annotations and docstrings of `connection` in `ReadSource`,
  `AppendOnly`, `FullLoad` and `SCD2`.
- No change to `pyproject.toml` or `uv.lock`: no extra, no package in the `dev` group (decision 14;
  decision 32 keeps it so).
- No change to `ingestio/__init__.py`, `ingestio/basics.py`, `connector/core.py` or the other
  connector modules.

### What becomes outdated

The documentation stage updates `docs/` and `.claude/kb/`; the README is the maintainer's.

- `docs/concepts/connectors.md`: a Snowflake section (connection and its credentials, names,
  reader, literals and the bound, time zones, options, writer, example, environments and
  privileges, limitations); the Extras section, whose rule 4 names the Snowflake Connector for
  Spark among the prerequisites and says that the Snowflake connectors need no extra.
- `docs/concepts/configuration.md`: the tables of systems, keys and callback arguments; the
  description of `connection`; the lists of `<names>`; the type-check message; `SCD2`'s list; the
  Extras paragraph.
- `docs/concepts/callbacks.md`: `connection` of the four callbacks and `SCD2`'s supported targets.
- ADR 0002, ADR 0012, ADR 0014 and ADR 0015: see [ADRs](#adrs). `docs/index.md`: this spec, and the
  description of the connectors page.
- `.claude/kb/architecture/quick-reference.md`: modules, configuration, public API, known gaps.
- Tests of other specs that list the systems; the test-writer of this spec updates them, with no
  change to what they check: `SOURCE_NAMES` in `tests/test_config_connectors.py`, `TARGET_NAMES` in
  `tests/test_append_only_callback.py`, `SOURCE_NAMES` and `TARGET_NAMES` in
  `tests/test_mongodb_reader.py`, `SOURCE_NAMES`, `TARGET_NAMES` and the `SCD2` list in
  `tests/test_mysql_connectors.py`, and `SUPPORTED` in `tests/test_scd2_callback.py`. No test uses
  `snowflake` as the name of a system that does not exist.
- Validated specs whose criteria quote these lists (among them `config_connectors`,
  `append_only_callback`, `scd2_callback`, `mongodb_reader` and `mysql_connectors`): the main
  session decides whether to amend their text.
- `README.md`: the connectors table gains a `snowflake` row, the Spark note names the Snowflake
  Connector for Spark among the packages a session may need, and the `SCD2` sentence gains
  `snowflake`, for the maintainer.

### Supported environments

(Decision 31.)

- **pyspark** 3.5 or later, a prerequisite.
- **The Snowflake Connector for Spark** from 3.2.1, with the **Snowflake JDBC driver** from 4.0.2,
  which 3.2.0 and later require (F11), both on the session's classpath, prerequisite JARs, for
  example `spark.jars.packages=net.snowflake:spark-snowflake_<Scala version>:<version>,net.snowflake:snowflake-jdbc:<version>`.
  The connectors check no version at run time.
- **Spark** 3.5, 4.0 and 4.1, the versions 3.2.0 adds or keeps (F11). Spark 4.2: no claim.
- **Databricks classic compute** on Databricks Runtime 15.4 LTS or later, in dedicated access mode.
  Databricks Runtime 17.3 LTS and 18 ship neither the connector nor the Snowflake JDBC driver (V9):
  the job installs both as cluster libraries, prerequisites that the library never installs. What
  the other runtimes ship was not read. Standard access mode, serverless compute and Databricks
  Connect: no claim. Only the DataFrame API is used.
- **Open-source Spark** 3.5 or later, on a Java release that Spark version supports, with both JARs
  on the classpath.
- **Snowflake**: any account the connector reaches; the connectors add no requirement of their own.
- **Network.** The driver and the executors reach the account, and the stage the connector uses.
- **Privileges**, as far as the planner knows (K10, to be confirmed): `USAGE` on the warehouse, the
  database and the schema; `SELECT` on a source table; `INSERT` on a target, and what `TRUNCATE`
  needs for `replace=True`; `CREATE STAGE` on the session's schema, for the connector's temporary
  stage (K15).

### Known limitations

These are documented, not solved here.

- **Late commits.** A row committed after a read, with a watermark at or below the bound that read
  captured, is never read by a later run. Rows that share the exact value of the previous watermark
  but commit later are missed the same way.
- **Full reads are not frozen.** Every action on a full read's DataFrame runs the query again.
- **Nanoseconds** (K8). Spark holds timestamps to the microsecond. A `TIMESTAMP_*` column whose
  values carry nanoseconds gives the job a watermark, computed from the DataFrame, below the rows it
  read, and the next run reads them again. The bound itself keeps the nanoseconds.
- **Three queries per watermark read**, one of which returns no row, each on the session's
  warehouse, which resumes if it is suspended.
- **`FLOAT` watermarks** (K5): that the text of a `FLOAT` bound converts back to the same value is
  not established. Other types, such as `BOOLEAN` or `TIME`, get the session's text of the bound.
- **Input formats** (K3): a user or account whose `DATE_INPUT_FORMAT` or `TIMESTAMP_INPUT_FORMAT`
  is not `AUTO` may read the literals otherwise: no claim.
- **`TRUNCATE` and load metadata** (F7). A replace deletes the target's load metadata, which cannot
  be recovered: a target also loaded by `COPY INTO` from an external stage, or by Snowpipe, may load
  the same files again later. The deleted rows stay in Time Travel for the retention period, and so
  in the account's storage.
- **DDL from other sessions** (F7). A DDL statement on the target from another session while the
  transaction runs is what Snowflake's documentation says to avoid.
- **A target dropped during a write** (F3, V1). Between the connector's metadata lookup and its own
  existence check, another session can drop the target; the connector then creates it from the
  DataFrame's schema before its transaction. The window is the length of the stage upload.
- **An empty DataFrame on a replace** (V4, decision 33): the connector runs its transaction, the
  `TRUNCATE` and a `COPY` over a prefix with no file. That this `COPY` succeeds with zero rows is not
  read in the source; the integration tier checks that the target ends up empty.
- **Columns** (V1). With the default `column_mismatch_behavior`, a write carries exactly the
  target's columns: a DataFrame with a column more or a column fewer fails, unlike `MySQLWriter`,
  which accepts a subset; a DataFrame or a target with two columns that differ only in case fails.
  `column_mismatch_behavior=ignore` leaves the unmatched columns out, and a target column left out
  takes its default or `NULL` (K6); `continue_on_error=on` lets the `COPY` skip rows it cannot load;
  both are the job's choice.
- **Spark task retries.** Whether a task re-run after its upload stages its partition twice, and the
  `COPY` loads it twice, is not established.
- **Names.** An unquoted reserved word fails (K1). With the connector at 3.2.1 or 3.2.2, columns
  whose names contain `"` behave differently than at 3.2.3, which quotes such a name with its
  embedded `"` doubled and unescapes `""` inside a quoted name (F11, V8).
- **Credentials in Spark's plans.** From 3.2.1 the connector registers its sensitive options with
  Spark's redaction (F11); with an older release, the key or the token may show in plans, logs or
  the Spark UI.
- **Authentication.** Only an unencrypted private key and an OAuth access token (decision 3). A key
  protected by a passphrase is decrypted by the job, for example with the `cryptography` package,
  before it builds the connection. Refreshing an expired token is the job's.
- **Not verified.** The facts K1 to K10, K12, K13, the residual K14, K15, K18 and K21 are the
  planner's knowledge, not established by a source read for this spec, and so is the `COPY` of no
  file of V4. The integration tests below check the ones they can; the others stay claims to
  confirm.

### Notes for testing

Every criterion has at least one test that always runs, with test doubles, without a JVM, a network
or a Snowflake account. No criterion needs Spark; only the integration tests do. The doubles live in
`tests/test_snowflake_connectors.py` and share one ordered event log.

- **Fake Spark session**, as in `tests/test_mysql_connectors.py`. Each access to `spark.read`
  returns a new fake reader, which supports `format(name)`, `option(key, value)` and
  `options(**kwargs)`, and records one *load* each time `load()` is called, with the format and the
  merged options: a later call replaces an earlier key with the same name, compared
  case-insensitively, as Spark does; the original case of every key is kept. Values are compared as
  Spark sees them: `str(value)`, and booleans as `true` and `false`.
- **Fake DataFrame.** `load()` returns a new fake DataFrame and records every attribute accessed on
  it. A DataFrame has a `schema`, a real `pyspark.sql.types.StructType` that the test chooses, by
  default `StructType([StructField("UPDATED_AT", TimestampType())])`, and answers `first()` with the
  row the test chose, by default `("2026-10-21T09:30:00.500000000+00:00",)`, or `None`; accessing
  `schema` and calling `first()` are events. Importing `pyspark.sql.types` starts no JVM. A
  DataFrame given to the writer has `columns` and `write`; `write` returns a fake writer that
  supports `format`, `option`, `options` and `mode`, and records a *save*, with its format, mode and
  merged options. Any of these calls can be made to raise.
- **Keys.** The tests need no real key: the body is any base64 text, such as
  `KEY = base64.b64encode(bytes(range(100))).decode()`, and the PEM text is that body split into
  lines of 64 characters between the two armour lines. No package outside the standard library is
  needed.
- `pyspark.sql.SparkSession.active` can be replaced with `monkeypatch.setattr`. A missing package is
  simulated in a subprocess, or with `monkeypatch.setitem`, by mapping its name to `None` in
  `sys.modules`.
- **Configurations** are written with `json.dumps` to a file under `tmp_path`, as in
  `tests/test_config_connectors.py`. A *twin* is a connector built directly from
  `ingestio.connector.all` over a second set of fakes in the same initial state.
- Tests use the public API, and pass the fakes as `spark=` and as DataFrames, with `typing.cast` to
  satisfy mypy strict, which checks `tests/` too.

### Integration tests

(Decision 32.) On top of the criteria, real tests against a Snowflake account, marked
`@pytest.mark.integration` and opt-in, skipped with a reason unless all of these are set:

- `INGESTIO_SPARK_TESTS` is `1`;
- `INGESTIO_SNOWFLAKE_PACKAGES` holds the Maven coordinates of the connector and of the JDBC
  driver, separated by a comma, which a local Spark session gets through `spark.jars.packages`;
- `INGESTIO_SNOWFLAKE_HOST`, `INGESTIO_SNOWFLAKE_USER`, `INGESTIO_SNOWFLAKE_PRIVATE_KEY` (the PEM
  text), `INGESTIO_SNOWFLAKE_DATABASE`, `INGESTIO_SNOWFLAKE_SCHEMA` and
  `INGESTIO_SNOWFLAKE_WAREHOUSE`, and `INGESTIO_SNOWFLAKE_ROLE` when the account needs one.

The file runs in its own process, as the Unity Catalog and BigQuery integration tests do. Each test
creates its own uniquely named tables and drops them, with statements run through the connector's
`Utils.runQuery` over py4j (K21), so no Python client is installed. They are written blind where the
stage that writes them cannot run them, and run in CI once the maintainer adds the account's
secrets.

1. A full read of a table with `NUMBER(38,0)`, `NUMBER(10,2)`, `VARCHAR`, `DATE`,
   `TIMESTAMP_NTZ(9)`, `TIMESTAMP_LTZ(9)`, `TIMESTAMP_TZ(9)` and `VARIANT` columns returns every row.
2. A watermark read on a `NUMBER(38,0)` column with values above 2^53 returns exactly the rows above
   the value; a row inserted after `read()` with a higher watermark is absent from a second action on
   the same DataFrame; a row with a `NULL` watermark is never returned.
3. Watermark reads on `TIMESTAMP_NTZ(9)`, `TIMESTAMP_LTZ(9)` and `TIMESTAMP_TZ(9)` columns, with an
   aware `datetime` value at the offset `-03:00`, return exactly the expected rows; a row inserted
   after `read()` one nanosecond above the bound is absent from a second action (K3, K4).
4. A watermark read on a `VARCHAR` column with a value and a bound that hold `'` and `\` returns
   exactly the expected rows (K2).
5. A table `"O""Brien"` in a schema `"Mixed Case"` is read with the `table_name`
   `"Mixed Case"."O""Brien"`; a table created unquoted is read with its name in lower case (K1).
6. `write` appends; the DataFrame's columns, in another order than the target's, land in the columns
   of the same names (V1); a DataFrame without one of the target's columns fails, and with
   `column_mismatch_behavior=ignore` that column takes its default (V1, K6).
7. `write(df, replace=True)` leaves exactly the DataFrame's rows. With an empty DataFrame that has
   the target's columns, it leaves the target empty (V4, decision 33).
8. `write` and `write(df, replace=True)` to a missing target fail, and no table of that name exists
   afterwards (V1).
9. A value that the target's column cannot hold, such as a text too long for `VARCHAR(3)`, fails the
   `COPY`; the target is unchanged, for an append and for a replace (F3).
10. A DataFrame column that the target lacks fails; the target is unchanged (F1, V1).
11. An `SCD2` run on a dimension with `TIMESTAMP_LTZ` version columns keeps the instants of the
    versions it does not change.

## Acceptance criteria

Every criterion follows the [Decisions](#decisions); a criterion that rests on one of decisions 13
to 34 names it. Unless a criterion says otherwise:

- `KEY = base64.b64encode(bytes(range(100))).decode()`, and `PEM` is
  `"-----BEGIN PRIVATE KEY-----\n"`, then `KEY` split into lines of 64 characters each ended by
  `"\n"`, then `"-----END PRIVATE KEY-----\n"`;
- `conn = SnowflakeConnection("acme-shop.snowflakecomputing.com", "shop", user="ingest", private_key=PEM)`;
- `oauth = SnowflakeConnection("acme-shop.snowflakecomputing.com", "shop", oauth_token="t0ken-secret")`;
- `spark` is the fake session, and DataFrames are the doubles of
  [Notes for testing](#notes-for-testing);
- the reader is `SnowflakeReader("shop.public.orders", conn, spark=spark)`, and the writer is
  `SnowflakeWriter("shop.public.orders", conn)`;
- the **connection options** of `conn` are `sfURL` = `acme-shop.snowflakecomputing.com`,
  `sfDatabase` = `shop`, `sfUser` = `ingest` and `pem_private_key` = `KEY`;
- `Q(t)` is the literal of [Literals](#literals);
- `df` is a fake DataFrame with `columns == ["id", "payload"]`;
- "exactly the options" means these keys and no others, compared case-insensitively;
- `path` is a configuration file under `tmp_path`, `<path>` is `os.fspath(path)`, and the default
  sections are `{"system": "snowflake", "table_name": "shop.public.orders"}` as `source` or as
  `target`;
- every error of a configuration is raised by the call that builds the callback, before any
  `Process` exists.

### Import surface and the extra

- **AT-001**: Given a fresh interpreter, when
  `from ingestio.connector.all import SnowflakeConnection, SnowflakeReader, SnowflakeWriter` runs,
  then it succeeds; the three names are in `ingestio.connector.all.__all__` and in
  `ingestio.connector.snowflake.__all__` (decision 21), and none of them is in `ingestio.__all__`.
- **AT-002**: Given an interpreter where `import pyspark` raises `ImportError`, when
  `ingestio.connector.all` is imported, then the import succeeds; `conn`, `oauth`, a
  `SnowflakeReader` with `spark=<fake session>` and a `SnowflakeWriter` can be built; the reader's
  full read returns the DataFrame of the fake's load; and `write(df)` and `write(df, replace=True)`
  record their saves.
- **AT-003**: Given the metadata of the installed distribution,
  `importlib.metadata.metadata("ingestio")`, then `snowflake` is not among its `Provides-Extra`
  values, and no `Requires-Dist` carries the marker `extra == "snowflake"` (decision 14).

### `SnowflakeConnection`

- **AT-004**: Given the calls below, then each raises `ValueError` with the message shown
  (decisions 23 and 24):

  | Call | Message |
  |---|---|
  | `SnowflakeConnection("", "shop", user="ingest", private_key=PEM)` | `host must not be empty` |
  | `SnowflakeConnection("h", "", user="ingest", private_key=PEM)` | `database must not be empty` |
  | `SnowflakeConnection("h", "shop", user="", private_key=PEM)` | `user must not be empty` |
  | `SnowflakeConnection("h", "shop", user="ingest", private_key=PEM, schema="")` | `schema must not be empty` |
  | the same with `warehouse=""` | `warehouse must not be empty` |
  | the same with `role=""` | `role must not be empty` |
  | `SnowflakeConnection("h", "shop", user="ingest")` | `give private_key or oauth_token` |
  | `SnowflakeConnection("h", "shop", user="ingest", private_key=PEM, oauth_token="t")` | `give private_key or oauth_token, not both` |
  | `SnowflakeConnection("h", "shop", oauth_token="")` | `oauth_token must not be empty` |
  | `SnowflakeConnection("h", "shop", private_key=PEM)` | `private_key needs user` |

  `SnowflakeConnection("h", "shop", oauth_token="t")`, the same with `user="ingest"`, and `conn`
  with each of `schema="public"`, `warehouse="wh"` and `role="loader"` are accepted. A call that
  breaks two rules raises the message of the earlier row: `SnowflakeConnection("", "", user="")`
  raises `host must not be empty`, and `SnowflakeConnection("h", "shop", oauth_token="")` with
  `role=""` raises `role must not be empty`.
- **AT-005**: Given `user="ingest"` and the `private_key` on the left, then the connection is built
  and a full read's load carries `pem_private_key` = `KEY` exactly, or the build raises `ValueError`
  with the message on the right (decision 25):

  | `private_key` | Result |
  |---|---|
  | `PEM` | `KEY` |
  | `PEM` with every `"\n"` replaced by `"\r\n"` | `KEY` |
  | `"  \n" + PEM + "\n\n"` | `KEY` |
  | `KEY` | `KEY` |
  | `KEY` split into lines of 64 characters | `KEY` |
  | `PEM` with `PRIVATE KEY` replaced by `ENCRYPTED PRIVATE KEY` in both armour lines | `private_key must be unencrypted` |
  | `PEM` with `PRIVATE KEY` replaced by `RSA PRIVATE KEY` in both armour lines | `private_key must be PKCS#8 (BEGIN PRIVATE KEY), not PKCS#1 (BEGIN RSA PRIVATE KEY)` |
  | `PEM` without its last line | `private_key must be an unencrypted PKCS#8 key, as PEM text or its base64 body` |
  | `"-----BEGIN CERTIFICATE-----\n" + KEY + "\n-----END CERTIFICATE-----"` | the same message |
  | `"-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----"` | the same message |
  | `""` and `"   "` | the same message |
  | `KEY[:-1] + "!"` | the same message |

  No message contains `KEY` or any 64-character line of it.
- **AT-006**: Then `repr(conn)` and `str(conn)` are
  `SnowflakeConnection(host='acme-shop.snowflakecomputing.com', database='shop', schema=None, warehouse=None, role=None, user='ingest')`;
  `repr(oauth)` is the same with `user=None`; `repr(SnowflakeReader("shop.public.orders", conn))`
  is
  `SnowflakeReader(table_name='shop.public.orders', host='acme-shop.snowflakecomputing.com', database='shop')`;
  and `repr(SnowflakeWriter("shop.public.orders", conn))` is
  `SnowflakeWriter(table_name='shop.public.orders', host='acme-shop.snowflakecomputing.com', database='shop')`.
  Given in addition `schema="public"`, `warehouse="wh"` and `role="loader"`, the connection's `repr`
  shows them as `'public'`, `'wh'` and `'loader'`; given a watermark and `options`, the `repr` of the
  reader and the writer is unchanged.
- **AT-007**: Given `conn`, then its attributes `host`, `database`, `user`, `private_key`,
  `oauth_token`, `schema`, `warehouse` and `role` are `"acme-shop.snowflakecomputing.com"`, `"shop"`,
  `"ingest"`, `PEM`, `None`, `None`, `None` and `None`, and assigning to any of them raises
  `dataclasses.FrozenInstanceError`. `SnowflakeConnection("h", "shop", "ingest")` raises `TypeError`:
  `user` is keyword-only.
- **AT-008**: When a full read runs with each connection on the left, then its load carries exactly
  the connection options on the right, plus `dbtable` (decisions 22 to 24):

  | Connection | Connection options |
  |---|---|
  | `conn` | `sfURL`, `sfDatabase`, `sfUser`, `pem_private_key`, as in the defaults |
  | `oauth` | `sfURL`, `sfDatabase`, `sfAuthenticator` = `oauth`, `sfToken` = `t0ken-secret` |
  | `oauth` built with `user="ingest"` | the same plus `sfUser` = `ingest` |
  | `conn` built with `schema="public"`, `warehouse="wh"`, `role="loader"` | the defaults plus `sfSchema` = `public`, `sfWarehouse` = `wh`, `sfRole` = `loader` |

### Building the connectors

- **AT-009**: Given `name` on the left, when `SnowflakeReader(name, conn)` or
  `SnowflakeWriter(name, conn)` is built, then it is accepted, or raises
  `ValueError("table_name must be 'table', 'schema.table' or 'database.schema.table', each part a Snowflake identifier")`
  (decisions 26 and 27):

  | Accepted | Rejected |
  |---|---|
  | `orders`, `public.orders`, `shop.public.orders`, `_t$1`, `ORDERS`, `"Mixed Case"`, `"Shop"."Sales.Q1"."O""Brien"`, `""""` (one `"`, quoted) | the empty string, `orders.`, `.orders`, `a.b.c.d`, `my table`, `1orders`, `$orders`, `"unterminated`, `a"b`, `"a"b"`, `""` (an empty quoted identifier), `shop."".orders`, `(SELECT 1)`, `orders;`, `ordérs` |

- **AT-010**: Given `SnowflakeReader("shop.public.orders", conn, watermark_value=41)`, then it raises
  `ValueError("watermark_value needs watermark_column")`. Given `watermark_column=""`, then it
  raises `ValueError("watermark_column must not be empty")`. Given `watermark_column` in
  `updated at`, `a.b`, `"x`, `9lives` and `x; DROP TABLE t`, then it raises
  `ValueError("watermark_column must be a Snowflake identifier")`; `updated_at`, `UPDATED_AT`,
  `"Updated At"` and `"a""b"` are accepted.
- **AT-011**: Given `watermark_column="updated_at"` and the `watermark_value` on the left, when the
  reader is built, then it raises `ValueError` with the message on the right:

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
- **AT-012**: Given `key` on the left, when the connector on the right is built with
  `options={key: "x"}`, then it raises `ValueError` with the message
  `option '<key>' is set by the connector`, with the key as given (decisions 30 and 34):

  | `key` | Connector |
  |---|---|
  | `sfURL`, `SFURL`, `sfDatabase`, `sfSchema`, `sfWarehouse`, `sfRole`, `sfUser`, `sfPassword`, `pem_private_key`, `PEM_PRIVATE_KEY`, `sfAuthenticator`, `sfToken`, `sfaccount`, `sfworkloadidentityprovider`, `oauthclientid`, `oauthclientsecret`, `oauthtokenrequesturl`, `oauthscope`, `connection_id`, `dbtable`, `Query` | both |
  | `usestagingtable`, `TRUNCATE_TABLE`, `column_mapping`, `columnmap`, `preactions`, `PostActions`, `internal_skip_write_when_writing_empty_dataframe` | `SnowflakeWriter` |

  `SnowflakeReader` accepts
  `{"autopushdown": "off", "sfTimezone": "UTC", "preactions": "ALTER SESSION SET QUERY_TAG = 'x'", "usestagingtable": "on"}`;
  `SnowflakeWriter` accepts
  `{"column_mismatch_behavior": "ignore", "continue_on_error": "on", "keep_column_case": "on", "sfCompress": False, "parallelism": 8, "sfTimezone": "UTC"}`.
- **AT-013**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises, when
  readers, with and without a watermark and `spark`, and writers, with and without `options`, are
  built, then each succeeds, no load or save is recorded, no attribute of `spark` is accessed and
  `active` is not called.

### Full read

- **AT-014**: When `reader.read()` runs, then exactly one load is recorded, with the format
  `snowflake` and exactly the options: the connection options and `dbtable` = `shop.public.orders`.
  `read()` returns the very DataFrame that `load()` returned, and no attribute of it was accessed.
- **AT-015**: Given `options={"autopushdown": "off", "sfTimezone": "UTC", "use_copy_unload": True}`,
  when the reader reads, then the load carries those three options, with the values `off`, `UTC` and
  `true`, besides the options of AT-014. Given a watermark too, the three loads of the watermark read
  carry them.
- **AT-016**: Given a reader built without `spark`, while `pyspark.sql.SparkSession.active` is
  replaced with a function that raises, then building succeeds. When `active` is then replaced with a
  function that returns the fake session and `read()` runs, then the load is recorded on the fake.
  Given `active` replaced with a function that raises `err` when `read()` runs, then `read()` raises
  `err` itself.

### Watermark read

The reader in this section is
`SnowflakeReader("shop.public.orders", conn, spark=spark, watermark_column="updated_at", ...)`. The
type load's DataFrame has the default schema of the doubles, and the bound load's DataFrame answers
`("2026-10-21T09:30:00.500000000+00:00",)`, unless a criterion says otherwise.

- **AT-017**: Given `watermark_value=datetime(2026, 10, 1, tzinfo=UTC)`, when `read()` runs, then the
  event log shows, in this order (decision 28):
  1. a load with the format `snowflake` and exactly the connection options and `query` =

     ```text
     SELECT updated_at FROM shop.public.orders WHERE FALSE
     ```

  2. one access to `schema` on that load's DataFrame, and no other attribute of it;
  3. a load with the format `snowflake` and exactly the connection options and `query` =

     ```text
     SELECT TO_VARCHAR(MAX(updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM') AS UPPER_BOUND FROM shop.public.orders WHERE updated_at > '2026-10-01T00:00:00+00:00'
     ```

  4. one `first()` on that load's DataFrame;
  5. a load with the format `snowflake` and exactly the connection options and `query` =

     ```text
     SELECT * FROM shop.public.orders WHERE updated_at > '2026-10-01T00:00:00+00:00' AND updated_at <= '2026-10-21T09:30:00.500000000+00:00'
     ```

  `read()` returns the third load's DataFrame, and no attribute of it was accessed. No other load is
  recorded, and no load carries `dbtable`.
- **AT-018**: Given no `watermark_value`, when `read()` runs, then the second load's `query` is
  `SELECT TO_VARCHAR(MAX(updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM') AS UPPER_BOUND FROM shop.public.orders`
  with nothing after the table, and the third load's `query` is
  `SELECT * FROM shop.public.orders WHERE updated_at <= '2026-10-21T09:30:00.500000000+00:00'`.
- **AT-019**: Given a bound load whose `first()` answers `(None,)`, and given one whose `first()`
  answers `None`, with or without a `watermark_value`, when `read()` runs, then the third load's
  `query` is `SELECT * FROM shop.public.orders WHERE FALSE`, and `read()` returns that load's
  DataFrame.
- **AT-020**: Given the `watermark_value` on the left, when `read()` runs, then the second and the
  third loads contain `updated_at > ` followed by the literal on the right (decision 20):

  | `watermark_value` | Literal |
  |---|---|
  | `41` | `41` |
  | `0` | `0` |
  | `-7` | `-7` |
  | `Decimal("12.50")` | `12.50` |
  | `Decimal("1E+3")` | `1000` |
  | `Decimal("-0.25")` | `-0.25` |
  | `"41"` | `'41'` |
  | `"O'Brien"` | `'O''Brien'` |
  | `r"C:\temp"` | `'C:\\temp'` |
  | `"it\\'s"` (the text `it\'s`) | `'it\\''s'` |
  | `""` | `''` |
  | `"é"` | `'é'` |
  | `date(2026, 10, 1)` | `'2026-10-01'` |
  | `datetime(2026, 10, 1, 8, 30, tzinfo=UTC)` | `'2026-10-01T08:30:00+00:00'` |
  | `datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-3)))` | `'2026-10-01T11:30:00.123456+00:00'` |

- **AT-021**: Given `watermark_column="w"`, no `watermark_value`, a type load whose schema's first
  field has the watermark type of a row, and a bound load whose `first()` answers that row's bound
  text, when `read()` runs, then the first load's `query` is
  `SELECT w FROM shop.public.orders WHERE FALSE`, the second load's `query` is
  `SELECT <render> AS UPPER_BOUND FROM shop.public.orders` with the row's render, and the third
  load's `query` is `SELECT * FROM shop.public.orders WHERE w <= <literal>` with the row's literal
  (decisions 20 and 28):

  | Watermark type | `<render>` | Bound text | `<literal>` |
  |---|---|---|---|
  | `LongType()` | `TO_VARCHAR(MAX(w))` | `9007199254740993` | `9007199254740993` |
  | `DecimalType(38, 0)` | `TO_VARCHAR(MAX(w))` | `18446744073709551615` | `18446744073709551615` |
  | `DecimalType(10, 2)` | `TO_VARCHAR(MAX(w))` | `12.50` | `12.50` |
  | `DoubleType()` | `TO_VARCHAR(MAX(w))` | `1.5e+20` | `1.5e+20` |
  | `DoubleType()` | `TO_VARCHAR(MAX(w))` | `inf` | `'inf'` |
  | `LongType()` | `TO_VARCHAR(MAX(w))` | `1 OR 1=1` | `'1 OR 1=1'` |
  | `StringType()` | `TO_VARCHAR(MAX(w))` | `41` | `'41'` |
  | `StringType()` | `TO_VARCHAR(MAX(w))` | `it's \x` | `'it''s \\x'` |
  | `BooleanType()` | `TO_VARCHAR(MAX(w))` | `true` | `'true'` |
  | `DateType()` | `TO_VARCHAR(MAX(w), 'YYYY-MM-DD')` | `2026-10-21` | `'2026-10-21'` |
  | `TimestampType()` | `TO_VARCHAR(MAX(w), 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')` | `2026-10-21T06:30:00.123456789-03:00` | `'2026-10-21T06:30:00.123456789-03:00'` |
  | `TimestampNTZType()` | `TO_VARCHAR(MAX(w), 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')` | `2026-10-21T09:30:00.000000000+00:00` | `'2026-10-21T09:30:00.000000000+00:00'` |
- **AT-022**: Given the `table_name` and `watermark_column` on the left, when a full read runs, its
  `dbtable` is the table name as given. When a watermark read with `watermark_value=1` runs, the
  three queries contain the table name and the column name exactly as given:

  | `table_name` | `watermark_column` |
  |---|---|
  | `orders` | `updated_at` |
  | `public.orders` | `UPDATED_AT` |
  | `"Shop"."Sales.Q1"."O""Brien"` | `"Updated At"` |
  | `shop.public."my table"` | `"a""b"` |

  So the first case's type load is `SELECT updated_at FROM orders WHERE FALSE`, and the third case's
  data load is
  `SELECT * FROM "Shop"."Sales.Q1"."O""Brien" WHERE "Updated At" > 1 AND "Updated At" <= '2026-10-21T09:30:00.500000000+00:00'`.
- **AT-023**: Given a fake whose `load()` raises `err = RuntimeError("down")`, when `read()` runs,
  with or without a watermark, then it raises `err` itself, and no load is recorded after the one
  that raised. Given a type load whose `schema` access raises `err`, or a bound load whose `first()`
  raises `err`, then `read()` raises `err` itself and records no later load.
- **AT-024**: Given a reader with a watermark, when `read()` runs twice and the bound load answers
  `("2026-10-21T09:30:00.500000000+00:00",)` the first time and `("2026-10-22T10:00:00.000000000+00:00",)`
  the second, then six loads are recorded, each read's data load uses its own bound, and the first
  `read()`'s DataFrame is not the second's.

### Writing

- **AT-025**: When `writer.write(df)` runs, then exactly one save is recorded, with the format
  `snowflake`, the mode `append`, and exactly the options: the connection options, `dbtable` =
  `shop.public.orders`, `usestagingtable` = `off`, `truncate_table` = `on` and `column_mapping` =
  `name` (decision 15). No load is recorded, no option `preactions` is sent, the only attributes
  accessed on `df` are `columns` and `write`, `save()` is called once, and `write` returns `None`.
  `writer.write(df, replace=False)` gives the same event log.
- **AT-026**: When `writer.write(df, replace=True)` runs, then the event log is the one of AT-025,
  except that the save also carries `preactions` = `TRUNCATE TABLE %s`, and its mode is still
  `append`.
- **AT-027**: Given `options={"column_mismatch_behavior": "ignore", "sfCompress": False, "parallelism": 8}`,
  when `write(df)` and `write(df, replace=True)` run, then each save carries those three options,
  with the values `ignore`, `false` and `8`, besides the options of AT-025 and AT-026. Given the
  writer built with `oauth`, the save carries `oauth`'s connection options of AT-008 instead of
  `conn`'s.
- **AT-028**: Given a save that raises `err = RuntimeError("COPY failed")`, when `write(df)` or
  `write(df, replace=True)` runs, then it raises `err` itself, with no note added, and no other save
  or load is recorded.
- **AT-029**: Given a fake DataFrame with `columns == []`, when `write` runs, with or without
  `replace=True`, then it raises `ValueError("the DataFrame has no columns")`, and no save is
  recorded.
- **AT-030**: Given `SnowflakeWriter('"Shop".public."Orders"', conn)` and a DataFrame with
  ``columns == ["Id", "my col", 'a"b']``, when `write(df, replace=True)` runs, then the save carries
  exactly the options of AT-026, except that `dbtable` is `"Shop".public."Orders"`: the writer
  sends no column name and writes none into `preactions`.
- **AT-031**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises, when
  `write(df)` runs, then `active` is not called.

### In a configuration

- **AT-032**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises, when
  `ReadSource(path, connection=conn, spark=spark)` is built with the default `source`, then it
  succeeds, `active` is not called, and no attribute of `spark` is accessed. When
  `Process([Step("extract", [<that ReadSource>])]).run()` runs, then `state["records"]` is the
  DataFrame of the one load recorded, and the events equal those of the twin
  `SnowflakeReader("shop.public.orders", conn, spark=<twin session>).read()`. Given the section with
  `"watermark_column": "updated_at"` and `"options": {"autopushdown": "off"}` added, and
  `watermark_value=41` given to `ReadSource`, the events equal those of the twin built with the same
  arguments. mypy strict accepts `ReadSource(path, connection=conn)`,
  `AppendOnly(path, connection=conn)`, `FullLoad(path, connection=conn)` and
  `SCD2(path, connection=conn)` with `conn` annotated `SnowflakeConnection`, a `SnowflakeReader`
  bound to a variable annotated `Reader[DataFrame]`, and a `SnowflakeWriter` bound to one annotated
  `Writer[DataFrame]`.
- **AT-033**: Given the default `target` with `"options": {"parallelism": 8}` added, when
  `AppendOnly(path, connection=conn)` and `FullLoad(path, connection=conn)` are built, then the
  writer each built has the state, `vars(...)`, of the twin
  `SnowflakeWriter("shop.public.orders", conn, options={"parallelism": 8})`, its `connection` being
  `conn` itself. Given `SnowflakeWriter.write` replaced on the class with a recorder, when a process
  runs each callback over `state["records"] = df`, then `AppendOnly` calls `write` once with `df` and
  no keyword argument, and `FullLoad` calls it once with `df` and `replace=True`.
- **AT-034**: Given the section on the left, when the callback of its section is built with
  `connection=conn`, then it raises `ValueError` with the message on the right:

  | Section | Message |
  |---|---|
  | `{"system": "snowflake"}` as `source` | `<path>: source.table_name is required for system 'snowflake'` |
  | `{"system": "snowflake"}` as `target` | `<path>: target.table_name is required for system 'snowflake'` |
  | the default `source` with `"host": "h"` | `<path>: source.host is not a key of system 'snowflake'` |
  | the same with `"database"`, `"user"`, `"private_key"`, `"oauth_token"`, `"schema"`, `"warehouse"`, `"role"`, `"connection"`, `"watermark_value"` or `"dbtable"` as the key | `<path>: source.<key> is not a key of system 'snowflake'` |
  | the default `target` with `"watermark_column": "updated_at"` | `<path>: target.watermark_column is not a key of system 'snowflake'` |
  | the default `source` with `"table_name": 1` | `<path>: source.table_name must be a string` |
  | the default `source` with `"watermark_column": null` | `<path>: source.watermark_column must be a string` |
  | the default `target` with `"options": {"parallelism": [8]}` | `<path>: target.options must be an object of strings, numbers and booleans` |

- **AT-035**: When `ReadSource(path)` is built with the default `source`, then it raises
  `ValueError("<path>: source system 'snowflake' needs the argument connection")`; when
  `AppendOnly(path)` or `FullLoad(path)` is built with the default `target`, then it raises
  `ValueError("<path>: target system 'snowflake' needs the argument connection")`. Given
  `client=object()` with `connection=conn`, then `ReadSource` raises
  `<path>: source system 'snowflake' takes no argument client`, and `AppendOnly` and `FullLoad` raise
  `<path>: target system 'snowflake' takes no argument client`.
  `ReadSource(path, connection=conn, client=None, watermark_value=None, spark=None)` builds.
- **AT-036**: Given the callback, the section and the `connection` on the left, when the callback is
  built, then it raises `ValueError` with the message on the right:

  | Callback | Section | `connection` | Message |
  |---|---|---|---|
  | `ReadSource` | the default `source` | `MySQLConnection("db", "shop", "ingest", "pw")` | `<path>: source system 'snowflake' needs a SnowflakeConnection as the argument connection, not MySQLConnection` |
  | `ReadSource` | the default `source` | `MongoDBConnection("mongodb://db")` | `<path>: source system 'snowflake' needs a SnowflakeConnection as the argument connection, not MongoDBConnection` |
  | `AppendOnly`, `FullLoad` | the default `target` | `PostgresConnection("db", "shop", "ingest", "pw")` | `<path>: target system 'snowflake' needs a SnowflakeConnection as the argument connection, not PostgresConnection` |
  | `ReadSource` | `{"system": "mysql", "table_name": "shop.orders"}` as `source` | `conn` | `<path>: source system 'mysql' needs a MySQLConnection as the argument connection, not SnowflakeConnection` |
  | `ReadSource` | `{"system": "mongodb", "database": "shop", "collection": "orders"}` as `source` | `conn` | `<path>: source system 'mongodb' needs a MongoDBConnection as the argument connection, not SnowflakeConnection` |
  | `AppendOnly`, `FullLoad` | `{"system": "postgres", "table_name": "public.orders"}` as `target` | `conn` | `<path>: target system 'postgres' needs a PostgresConnection as the argument connection, not SnowflakeConnection` |

  A `ReadSource` and an `AppendOnly` given an instance of a subclass of `SnowflakeConnection` build.
  No message holds `KEY` or `t0ken-secret`.
- **AT-037**: Given a `source` whose `system` is `"Snowflake"`, when `ReadSource(path)` is built,
  then it raises
  `ValueError("<path>: source.system 'Snowflake' is not one of: bigquery, disk, gcs, json_api, mongodb, mysql, mysql_cdc, postgres, s3, snowflake, unity_catalog, volume")`.
  Given a `target` whose `system` is `"snowpipe"`, when `AppendOnly(path)` is built, then it raises
  `ValueError("<path>: target.system 'snowpipe' is not one of: bigquery, disk, gcs, json_lines, mysql, postgres, s3, snowflake, unity_catalog, volume")`.
- **AT-038**: Given the section on the left, when the callback of its section is built with
  `connection=conn`, then it raises the error on the right, the same object that the connector
  raises, with exactly one note, `while building the <section> of <path>, system 'snowflake'`:

  | Section | Error |
  |---|---|
  | the default `source` with `"table_name": "a.b.c.d"` | the `ValueError` of AT-009 |
  | the default `source` with `"watermark_column": "a b"` | `ValueError("watermark_column must be a Snowflake identifier")` |
  | the default `source` with `"options": {"dbtable": "x"}` | `ValueError("option 'dbtable' is set by the connector")` |
  | the default `target` with `"options": {"preactions": "x"}` | `ValueError("option 'preactions' is set by the connector")` |

### `SCD2`

- **AT-039**: Given the file
  `{"target": {"system": "snowflake", "table_name": "shop.public.dim_customer"}, "scd2": {"key_columns": ["customer_id"], "effective_time_column": "updated_at"}}`,
  when `SCD2(path, connection=conn)` is built, then it succeeds (decision 4). `SCD2(path)` raises
  `ValueError("<path>: target system 'snowflake' needs the argument connection")`, and
  `SCD2(path, connection=<the MySQLConnection of AT-036>)` raises
  `ValueError("<path>: target system 'snowflake' needs a SnowflakeConnection as the argument connection, not MySQLConnection")`.
  Given the `target` `{"system": "disk", "path": "/data/dim", "format": "parquet"}` instead, then
  `SCD2(path)` raises
  `ValueError("<path>: target.system 'disk' is not one of: bigquery, mysql, postgres, snowflake, unity_catalog")`.
- **AT-040**: Given the file of AT-039, a fake batch in `state["records"]` whose `columns` are
  `["customer_id", "name", "city", "updated_at"]` and whose `sparkSession` is a fake session, and
  `SnowflakeReader.read` replaced on the class with a function that records the reader and raises
  `err`, when a process runs `SCD2(path, connection=conn)`, then it raises `err` itself; the recorded
  reader has the state, `vars(...)`, of the twin
  `SnowflakeReader("shop.public.dim_customer", conn, spark=<the batch's session>)`; and the writer's
  `write` is not called.

### Logging and secrets

- **AT-041**: Given `caplog` capturing every level, when connections, readers, writers and the
  callbacks of AT-032 to AT-040 are built, `read()` runs as a full read, as a watermark read and with
  a bound of `None`, and `write` runs successfully and with a failed save, then no log record is
  captured.
- **AT-042**: Given the connections, connectors and callbacks that AT-004 to AT-040 build, then
  neither `KEY`, nor any 64-character line of it, nor `t0ken-secret` appears in any `repr` or `str` of
  them, or in the message or the notes of any error those criteria raise.

## Decisions

Taken by the maintainer on 2026-11-18, before planning. The maintainer gave no technical reason
beyond what the options stated; the reasons below are what the evidence (F1 to F13) establishes,
and `TODO(maintainer)` marks a reason that neither the maintainer nor the evidence gives.

1. **Engine: the Snowflake Connector for Spark** (`net.snowflake.spark.snowflake`, format
   `snowflake`), not Spark's generic JDBC source and not a Python-only client. The connector's JAR
   and the Snowflake JDBC driver are prerequisites that the runtime provides, as MySQL Connector/J
   is for the MySQL connectors. The reader returns a Spark DataFrame; the writer takes one.
   - Rationale: `TODO(maintainer)`, for the choice of this connector over the generic JDBC source and
     over a Python client. What the evidence adds: the connector has a write path that truncates and
     loads in one transaction (F2, F3), on which decision 2 rests.
2. **Write atomicity: the connector's own transaction.** The writer reserves `truncate_table=on` and
   `usestagingtable=off`, which routes the connector to its transactional path: `START
   TRANSACTION`, a `TRUNCATE` on a replace, `COPY INTO`, `COMMIT`, and a rollback on failure. No
   driver-side Python client, so no snowflake-connector-python and no extra for moving data. The
   connector's default staging and swap path is rejected. The writer runs no DDL on the target, and a
   missing target fails.
   - Rationale, from the evidence: the routing needs both options (F2); the transactional path runs
     the truncation and the load in one user transaction and rolls both back (F3), and `TRUNCATE` is
     DML in Snowflake, so it commits with the load (F7). The default path swaps the table object and,
     with the default options, recreates the column types from the DataFrame (F4), against
     [ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md);
     ADR 0014 also rules out creating a Snowflake table from a DataFrame, which needs a type mapping
     chosen by the connector. How a missing target fails is decision 15.
3. **Authentication: a key pair or an OAuth access token only.** `SnowflakeConnection` supports an
   unencrypted private key (`pem_private_key`) and an OAuth access token (`sfAuthenticator=oauth` and
   `sfToken`). No password, and no programmatic access token through the password field.
   - Rationale, from the evidence: Snowflake turns off single-factor password sign-in for all users,
     `LEGACY_SERVICE` included, in its final phase, August to October 2026 (F13). The connector's
     `pem_private_key` takes an unencrypted PKCS#8 key and no passphrase (F8). Why these two methods
     and not OAuth client credentials or workload identity, which the connector also offers (F8,
     F11), and why no encrypted key: `TODO(maintainer)`.
4. **`snowflake` is an `SCD2` target,** like `postgres`, `mysql`, `bigquery` and `unity_catalog`:
   `SCD2`'s `connection` widens to `SnowflakeConnection`, and its atomic rewrite uses the writer's
   replace.
   - Rationale: `TODO(maintainer)`. The spec checks the rule of decision 26 of the SCD2 spec under
     [SCD2](#scd2): it holds by V4.

Followed from what the shipped connectors settled. Each follows the precedent named with it and is
not an open question:

5. **The job names the system in its configuration:** one more row of the closed table in
   `src/ingestio/callback/_configuration.py`, for the source and the target; section keys are the
   constructor arguments of the same name; credentials and per-run values are callback arguments
   ([ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md)).
6. **The writer offers `write(data, *, replace=False)`** and decides nothing else; `AppendOnly`
   appends, `FullLoad` and `SCD2` replace
   ([ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md)).
7. **The target must already exist;** the writer runs no DDL on it
   ([ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md),
   and decision 2).
8. **The callbacks check the type of `connection` per system,** which widens `connection` of
   `ReadSource`, `AppendOnly`, `FullLoad` and `SCD2` to `SnowflakeConnection`
   ([specs/mongodb_reader.md](mongodb_reader.md), decision 10;
   [specs/mysql_connectors.md](mysql_connectors.md), decision 8).
9. **ADR 0012 gets a "leans towards batch" bullet** for these connectors (see [ADRs](#adrs)).
10. **The reader's recipe is `MySQLReader`'s:** the filter written into the SQL that the source
    runs; the slice frozen by an upper bound that a bound query captures as the source's text; the
    comparison strictly greater, and `NULL` watermarks never read; the five watermark value types,
    naive datetimes and floats rejected, aware datetimes normalised to UTC; one `options` mapping
    passed through, with the connector's keys reserved, and no `query=`, `columns=` or
    `primary_key`; an empty DataFrame through the same write. Only the literals, the names and the
    bound's text change, for Snowflake (decisions 20 and 26 to 28).
11. **The extras pattern** ([specs/postgres_connectors.md](postgres_connectors.md), decision 19;
    [ADR 0002](../docs/decisions/0002-zero-dependency-core.md)): an extra only for a package that a
    connector imports; lazy imports; an engine or a JAR that the runtime provides is a prerequisite,
    never an extra ([specs/mongodb_reader.md](mongodb_reader.md), decision 33).
12. **Tests with doubles, and opt-in integration tests.** Every criterion runs with test doubles.
    The tests that need Spark are opt-in through `INGESTIO_SPARK_TESTS=1` and run in CI
    ([specs/scd2_callback.md](scd2_callback.md), decision 34); the development machine cannot run
    Spark. Here only the integration tests need Spark (decision 32).

Proposed by this spec and accepted by the maintainer on 2026-11-18, as written. Each gives its
technical reason or the precedent it follows:

13. **Building does nothing but validate,** and each `read()` looks up the session and loads again,
    as the MySQL spec (decision 13) does, so that one reader serves every run.
14. **No extra.** The connectors import pyspark only, lazily: decision 2 leaves no Python client on
    the driver, and decision 3 needs no decryption of a key, so no connector imports a third-party
    package, and the pattern adds an extra only for one that a connector imports (decision 11). The
    Snowflake Connector for Spark and the Snowflake JDBC driver are JARs, prerequisites like the
    MongoDB Spark Connector. The private key is checked with `base64` from the standard library.
15. **The writer always saves in `append` mode with `column_mapping=name`; a replace adds
    `preactions=TRUNCATE TABLE %s`.** This settles how a missing target fails (decision 2).
    - Reason: with `column_mapping=name`, the connector resolves the target's columns from its
      metadata before it writes anything (F5): a missing target then fails while the connector
      prepares `select * from <dbtable> where 1 = 0`, at the start of `save()`, before any upload
      and before the `createTable` of the transactional path (F3, V1). The column map works in
      Append mode only (F5), so the replace cannot use Overwrite; the `preactions` run inside the
      connector's transaction, before the `COPY` (F3, V2), so the `TRUNCATE` commits with the load
      or rolls back with it, as the Overwrite path's own `TRUNCATE` would (F3). `%s` lets the
      connector put its own `dbtable` in the statement (F6, V2), so the writer writes no name into
      it. The map also matches the DataFrame's columns to the target's by name, as
      `PostgresWriter`, `MySQLWriter` and `BigQueryWriter` do; by position, the connector's default
      (F1), a DataFrame whose columns come in another order would load values into the wrong
      columns.
    - Rejected: an existence check of the writer's own, through a Spark read of the target before the
      save, which costs one more query and still leaves the target to be created if it disappears in
      between, and keeps the mapping by position; a check through the connector's `Utils.runQuery`
      over py4j, which has the same gap and needs the JVM gateway, which Spark Connect and
      Databricks' standard access mode do not expose (the planner's knowledge, to be confirmed).
    - Read since the first draft: V1 settles F5's inference, and V2 shows that F6 splits on `;`
      before it replaces `%s`.
16. **The filter is in the query that Snowflake runs:** the data load is a `query` load,
    `SELECT * FROM <table> WHERE <predicate>`, so it does not depend on the connector's
    `autopushdown` (F12), as `MySQLReader`'s subquery does not depend on Spark's predicate pushdown.
17. **The writer reads only `data.columns` and `data.write`, calls `save()` once, and never counts
    the DataFrame,** as the MySQL spec (decision 21); the `the DataFrame has no columns` check is
    `MySQLWriter`'s.
18. **Errors propagate unchanged, and the connectors write no log lines,** as the MySQL spec
    (decisions 20 and 22): a connector does not know the run id. The writer adds no note: it has no
    cleanup of its own, because the connector rolls back its transaction (F3).
19. **`SnowflakeConnection` is a frozen dataclass whose `repr` hides the key and the token,** as
    `MySQLConnection` hides its password; its `repr` shows every other field, `None` included, so
    that two connections that differ only in schema, warehouse or role print differently.
20. **Literals: numbers unquoted; text, dates and datetimes as single-quoted strings with `\` and
    `'` escaped; aware datetimes normalised to UTC and written in ISO 8601 with `+00:00`.**
    - Reason: in a Snowflake string constant `\` is an escape character and `''` a quote (K2), so
      writing both escaped keeps the value's text as it is, and no value can end the literal. An
      unquoted number compares exactly with a `NUMBER` (K5). An offset in the literal fixes the
      instant for `TIMESTAMP_LTZ` and `TIMESTAMP_TZ` whatever the session's zone (K3), which
      decision 29 leaves to the connector; on a `TIMESTAMP_NTZ` the UTC wall clock is compared, as
      `PostgresReader` and `MySQLReader` compare their zone-less columns. ISO 8601 is the form that
      the default input format `AUTO` reads (K3).

Taken by the maintainer on 2026-11-18 when answering the thirteen questions of the first draft: the
recommended option of questions 1 to 12, and option (c) of question 13, which V4 settles. They are
numbered after decision 20, so that the earlier numbers do not change: question *n* became decision
20 + *n*. The rationale of each is the technical reason of its option, which the maintainer took as
written; where that reason rested on a K fact that a V fact has since replaced, it cites the V fact.

21. **Names: `SnowflakeConnection`, `SnowflakeReader` and `SnowflakeWriter`; the system
    `snowflake`; the module `src/ingestio/connector/snowflake.py`** (question 1, option (a)).
    - Rationale: the product's name gives the system name and the module, as `mysql` and
      `bigquery` do, and the classes follow the `<System>Connection`, `<System>Reader`,
      `<System>Writer` pattern of the MySQL connectors (decision 26 of their spec).
22. **The account field is `host`: the account's host name, sent as `sfURL` as given, checked only
    for being non-empty** (question 2, option (a)).
    - Rationale: `sfurl` is the option the connector requires (F8), and an account's host name is
      not always derived from its identifier by a fixed suffix (K9), so taking it as given reaches
      every account with nothing built by the connector; the name is the one `PostgresConnection`
      and `MySQLConnection` give their server.
23. **`database` is required and positional; `schema`, `warehouse` and `role` are keyword-only and
    optional, sent only when given** (question 3, option (a)).
    - Rationale: a one- or two-part `table_name` resolves against the session's database (K1),
      which the MySQL spec requires for the same reason (decision 27 of its spec), and the connector
      requires `sfDatabase` on every connection (V6); `sfSchema` has the connector's default,
      `public` (F10), and a user's default warehouse and role apply when they are not set.
24. **One class; `private_key` and `oauth_token` keyword-only, exactly one of them given; `user`
    keyword-only and optional, required with `private_key`** (question 4, option (a)).
    - Rationale: the connector needs `sfuser` with a key and not with a token (F8), so the value
      asks for what the connector needs and no more; one class keeps one `connection` type for the
      callbacks' check.
25. **The private key is PEM text or its base64 body; the connection strips the armour lines and
    the whitespace, rejects the encrypted and the PKCS#1 forms by their first line, and checks that
    the body decodes as base64** (question 5, option (a)).
    - Rationale: the connector decodes the value as base64 and reads it as PKCS#8 (F8), so the
      armour lines would corrupt it (F8's inference), while a key generated for Snowflake is
      commonly stored as PEM text; the strip is a text operation with no package, and the checks
      turn a run-time `Input PEM private key is invalid` (F8) into an error at build time for the
      cases a first line tells apart.
26. **Names reach the SQL as Snowflake's SQL writes them:** each part of `table_name`, and
    `watermark_column`, is checked to be an unquoted identifier or a double-quoted one, and is
    written as given (question 6, option (a)).
    - Rationale: it is the form the connector itself expects in `dbtable` (F10), so one form serves
      `dbtable`, the reader's queries and the `%s` of the writer's `preactions` (F6, V2); it reaches
      every name Snowflake can hold, a folded one (`orders` for `ORDERS`) and a case-sensitive one
      (`"Orders"`); and the check keeps any other text out of the SQL (K1).
27. **`table_name` is `table`, `schema.table` or `database.schema.table`** (question 7, option
    (a)).
    - Rationale: Snowflake resolves a shorter name against the session's database and schema (K1),
      which the connection sets (decision 23), as the MySQL spec accepts MySQL's qualified forms
      (decision 28 of its spec).
28. **The bound's text: a type load, `SELECT <col> FROM <table> WHERE FALSE`, whose schema gives the
    watermark type with no row read; then a bound query whose render is chosen by that type:
    `'YYYY-MM-DD'` for a date, `'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM'` for a timestamp, `TO_VARCHAR`
    without a format otherwise** (question 8, option (a)).
    - Rationale: the bound must convert back to the same value for the slice to stay frozen; the
      connector sets the session's timestamp output formats itself (F9), to three fractional digits
      and, for `TIMESTAMP_NTZ`, no offset (V7), and an explicit format with nine fractional digits
      and the offset keeps the bound exact whatever those formats are (K4). Its cost is one more
      query, which reads no row.
29. **The connectors set no `sfTimezone`, which passes through; the connector sets the session to
    the JVM's zone** (question 9, option (a)).
    - Rationale: the literals and the bound carry their offsets (decisions 20 and 28), so the
      reader's comparisons do not depend on the zone; the zone the connector chooses governs how it
      moves the values themselves (F9), and changing it changes that handling in a way the evidence
      does not establish.
30. **Reserved options, as [Options](#options) lists:** the connection's and the authentication
    options, `dbtable` and `query`, on both connectors; `usestagingtable`, `truncate_table`,
    `column_mapping`, `preactions`, `postactions` and
    `internal_skip_write_when_writing_empty_dataframe` on the writer; everything else passes
    through, `column_mismatch_behavior` and `continue_on_error` included (question 10, option
    (a)). The option names that V5 gives (`sfaccount`, `sfworkloadidentityprovider`,
    `oauthclientid`, `oauthclientsecret`, `oauthtokenrequesturl`, `oauthscope`, `connection_id`) and
    the internal option of V4 complete the list, by the same reason.
    - Rationale: the reserved keys are those that carry the connection's values, would outrank or
      replace its credential (F8, V5), or that the writer's transaction and its missing-target
      guarantee depend on (F2, F5, F7, V1, V4); the two passing keys change which rows or columns
      reach the target, a choice the job makes explicitly.
31. **Supported versions: the connector from 3.2.1, the Snowflake JDBC driver from 4.0.2, Spark
    3.5, 4.0 and 4.1** (question 11, option (a)).
    - Rationale: from 3.2.1 the connector registers its sensitive options with Spark's log
      redaction (F11), which keeps the key and the token that this connection carries out of Spark's
      plans and logs, and it escapes quotes when it quotes identifiers (F11), which a quoted name may
      hold (decision 26); 3.2.0 and later require JDBC 4.0.2 (F11); Spark 3.5 to 4.1 are the
      versions 3.2.x builds for (F11). The source this spec relies on is the same from 3.2.1 to
      3.2.3, but for names that contain `"` (V8), a known limitation of 3.2.1 and 3.2.2.
32. **One opt-in tier of integration tests, with Spark and a real account,** as
    [Integration tests](#integration-tests) describes; tables created and dropped through the
    connector's `Utils.runQuery` over py4j (K21) (question 12, option (a)).
    - Rationale: the writer's transaction, the missing target, the forms of names, the literals and
      the bound's formats rest on behaviour that only a real account confirms, the K facts that
      remain and the `COPY` of no file of V4 among them, and `Utils.runQuery` needs no new package;
      the local Spark session that the tests start exposes py4j.
33. **An empty DataFrame on a replace needs nothing of the writer** (question 13, option (c), "the
    question does not arise").
    - Evidence: with `internal_skip_write_when_writing_empty_dataframe` at its default, false, which
      the writer reserves (decision 30), the connector still runs its transaction, the `TRUNCATE`
      preaction and the `COPY` when it uploaded no file (V4). So `FullLoad` of an empty batch empties
      a `snowflake` target, as it empties the other targets
      ([ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md)), provided that
      the `COPY` over a prefix with no file succeeds with zero rows, which is not read in the source
      and which the integration tier checks (decision 32).

Taken by the maintainer on 2026-11-18 when answering question 14, which V1 raised in the revision,
with its option (a):

34. **`columnmap` is reserved on the writer** (question 14, option (a)).
    - Rationale: decision 30 reserves the keys that the writer's missing-target guarantee depends on
      (decision 15), and V1 shows that the lookup on which that guarantee rests runs only when
      `columnmap` is absent.
    - Evidence: with a user's `columnmap` and the default CSV format, nothing looks up the target
      before the upload, and the transactional path creates a missing target from the DataFrame's
      schema (F3, V10), against ADR 0014.

### ADRs

- **No decision changes the philosophy, and none closes the door to streaming.** `Reader` and
  `Writer` are unchanged, and their payload stays open. No new ADR is needed.
- **ADR 0012, amended.** One more bullet in "What leans towards batch": the Snowflake connectors lean
  towards batch: `SnowflakeReader.read()` returns one slice, with a watermark fixed when the reader
  is built, and `SnowflakeWriter.write()` needs the whole DataFrame staged before its transaction
  commits. A streaming source or target for Snowflake, such as a stream on a table, Snowpipe
  Streaming or the connector's Structured Streaming write, would be a different connector. The
  doc-keeper writes it.
- **ADR 0015, amended.** Its Decision lists the system names, which gain `snowflake`, a source and a
  target. Its consequence on `connection` records that `ReadSource`, `AppendOnly`, `FullLoad` and
  `SCD2` take a `SnowflakeConnection` for `snowflake`, and that the type check covers it. The
  doc-keeper writes both, for the maintainer's approval.
- **ADR 0014 applies unchanged;** its consequences can name `SnowflakeWriter` among the writers that
  create nothing, the connector's temporary stage being the connector's own mechanism, and record
  how the writer keeps the connector from creating a missing target (decision 15).
- **ADR 0002 applies unchanged;** its consequences can record that the Snowflake connectors add no
  extra, their connector being a JAR that the runtime provides.

## Open questions

None.

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-11-18 | planning | draft | Drafted from the maintainer's decisions of 2026-11-18 (the Snowflake Connector for Spark; the connector's own transaction with `truncate_table=on` and `usestagingtable=off`; key pair and OAuth token only; `snowflake` as an `SCD2` target) and the source facts F1–F13 from the main session: `SnowflakeConnection`, `SnowflakeReader` with a watermark filter inside Snowflake over a slice frozen by an upper bound, and `SnowflakeWriter`, which always saves in Append mode with `column_mapping=name` and adds `preactions=TRUNCATE TABLE %s` on a replace; `snowflake` as a source, a target and an `SCD2` target; no extra. 42 criteria, 20 decisions (13–20 proposed), 13 open questions, and K1–K22 from the planner's knowledge, unverified. |
| 2026-11-18 | planning | draft | Revised with the maintainer's answers: the recommended option of questions 1–12 and option (c) of question 13 became decisions 21–33, and the proposed decisions 13–20 were accepted as written. The facts the main session read in the source, V1–V9, replace K11, K14 (but for its residual), K16, K17, K19, K20 and K22, settle F5's inference and the order of F6, and complete F9; by V1 a write must carry exactly the target's columns under the default `column_mismatch_behavior=error`; the reserved keys gain the V5 keys and `internal_skip_write_when_writing_empty_dataframe` (V4). V1 raised question 14. |
| 2026-11-18 | planning | draft | Revised with the maintainer's answer to question 14, option (a): decision 34 reserves `columnmap` on the writer, with V10 as its evidence (a user's `columnmap` skips the metadata lookup, so a missing target would be created). No open question left; 42 criteria, AT-001 to AT-042. |
| 2026-11-18 | approval | approved | Pre-approved by the maintainer on 2026-11-18 on the condition that the revision only records the answers and the verified facts; question 14, raised by the revision, was answered by the maintainer. The main session checked the revisions: decisions 21–34 match the answers, Open questions reads "None.", and the other changed lines are the rules V1, V4, V5, V8 and V9 correct and the references to decision 34 and V10. |
| 2026-11-18 | tests | tested | The test-writer wrote `tests/test_snowflake_connectors.py`: 76 tests and 293 cases covering AT-001 to AT-042, with a fake Spark session, fake readers, writers and DataFrames that follow PySpark's signatures and case-insensitive options, plus the opt-in tier of decision 32 (11 tests, 15 cases, skipped here: `INGESTIO_SPARK_TESTS` and the `INGESTIO_SNOWFLAKE_*` variables unset). It added `snowflake` to the system lists of `tests/test_config_connectors.py`, `tests/test_append_only_callback.py`, `tests/test_mongodb_reader.py`, `tests/test_mysql_connectors.py` and `tests/test_scd2_callback.py`. Every new or updated test fails only on the missing connectors or the missing `snowflake` system (29 existing tests among them), except AT-003, which asserts that no `snowflake` extra exists. The tier's set-up through `Utils.runQuery` (K21) and the Snowflake error wordings it matches for a missing table and a too-long value are unverified. |
| 2026-11-18 | documentation | documented | The doc-keeper documented the Snowflake connectors: a full section in `docs/concepts/connectors.md` after MySQL (the connection with its key pair and OAuth token, key text, names and case, the reader with the frozen bound, literals, time zones, options and reserved keys, the writer's transaction, the exact-columns rule and the missing target, an example, supported environments with the connector and JDBC JARs as prerequisites, cluster libraries on Databricks Runtime 17.3 LTS and 18, and known limitations), the note that the connectors need no extra; the `snowflake` system, `connection` widened to `SnowflakeConnection` and `snowflake` as an `SCD2` target in `configuration.md` and `callbacks.md`; the spec row in `docs/index.md`; amendments to ADRs 0002, 0012, 0014 and 0015 as the spec's ADRs section asks; and `.claude/kb/architecture/quick-reference.md`. The doc-keeper had no shell; the main session ran `tests/test_docs_examples.py`: 30 passed, and the two new Snowflake blocks fail only on the missing `SnowflakeConnection` import. |
| 2026-11-18 | implementation | approved | Returned to the test-writer. The implementer added `src/ingestio/connector/snowflake.py` (`SnowflakeConnection`, `SnowflakeReader`, `SnowflakeWriter`), the exports in `connector/all.py`, the `snowflake` rows and `SCD2` system in `callback/_configuration.py`, and the widened `connection` in `callback/io.py`; the suite passed except two defects of `tests/test_snowflake_connectors.py`, confirmed by the main session: `test_at041_nothing_writes_a_log_record` runs a `Process` whose own progress log (`ingestio.core`) writes 5 records, beyond AT-041, which only builds the callbacks and runs the connectors; and mypy reports `func-returns-value` at line 1681, where AT-026 binds the result of `SnowflakeWriter.write`, which returns `None`. |
| 2026-11-18 | tests | tested | Back from implementation: the test-writer fixed the two defects. AT-041's test no longer runs a `Process`; it builds the connections and the callbacks of AT-032 to AT-040, runs the reads and the writes, and captures no record, as the criterion says. AT-026's test calls `write` through the `EVERY_CALL` helper, which clears mypy's `func-returns-value`. With the implementation in place: 2190 passed, 155 skipped (the 15 opt-in Snowflake cases among them); ruff and mypy pass. |
| 2026-11-18 | documentation | documented | Run again after the return to the tests: the doc-keeper changed nothing. The only logging statement documented, "The connectors write no log lines.", matches AT-041's test as fixed, and AT-026's change is only how the test calls `write`. |
| 2026-11-18 | implementation | implemented | Run again after the return: the implementer changed nothing. `src/ingestio/connector/snowflake.py` holds `SnowflakeConnection` (a frozen dataclass: the checks in the spec's order, the key body taken from PEM text or base64, a `repr` without secrets, and the connector's options), `SnowflakeReader` (a full read, and a watermark read in three loads: type, bound and data) and `SnowflakeWriter` (always `append` with `usestagingtable=off`, `truncate_table=on` and `column_mapping=name`, plus `preactions=TRUNCATE TABLE %s` on a replace); `connector/all.py` exports them; `callback/_configuration.py` adds the `snowflake` source and target rows and the `SCD2` system; `callback/io.py` widens `connection`. 2190 passed, 155 skipped; ruff and mypy pass. The code relies on the unverified K1–K6 and K13, the empty COPY of V4 and F8's inference about the key body. |
| 2026-11-18 | validation | validated | Passed: AT-001 to AT-042 pass (278 passed, 15 skipped in the spec's file; the whole suite 2190 passed, 155 skipped, the documentation examples included); ruff, ruff format, mypy and `uv lock --check` pass. A static review of `snowflake.py` against F1–F13 and V1–V10 found the options, the reserved keys, the order of the loads, the literals, the transaction options, the missing-target guarantee and the handling of secrets as specified. Not run: the 11 opt-in integration tests of decision 32 (15 cases), for lack of Spark and of a Snowflake account; CI sets no `INGESTIO_SNOWFLAKE_*` variable, and the tier needs a process of its own. Non-blocking notes: two for the doc-keeper ("Neither have the Snowflake connectors" at `docs/concepts/connectors.md:4464`; the key's line ends at :2455 describe less than `str.splitlines()` accepts) and two cosmetic ones for the implementer (the user's options merged twice on a full read; the key parsed again on every load). |
| 2026-11-18 | validation | tested | Returned to the doc-keeper to settle the validator's non-blocking notes before the commit, as the maintainer asks for every validation with notes; the implementer's two notes follow. |
| 2026-11-18 | documentation | documented | The doc-keeper settled its two notes in `docs/concepts/connectors.md`: "Neither have the Snowflake connectors" became "Nor do the Snowflake connectors", and the key's line ends are described as the spec states them, the lines split as Python's `str.splitlines()` splits them. Nothing else changed. |
| 2026-11-18 | implementation | implemented | The implementer settled its notes: the full read no longer merges the user's options before `_load`, which merges them in one place, with the same options and precedence. The key is parsed once per `read()` or `write()`, since both reuse one `_spark_options()` across their loads, so the second note needed no change. 2190 passed, 155 skipped; ruff and mypy pass. With the maintainer's approval of 2026-11-18, the main session applied the README patch (the `snowflake` row, the Snowflake Connector for Spark and JDBC driver in the Spark note, `snowflake` among the `SCD2` targets) and amended the criteria that quote the lists of systems in the validated specs `append_only_callback` (AT-015), `scd2_callback` (AT-005), `mongodb_reader` (AT-030) and `mysql_connectors` (AT-045, AT-047); `config_connectors` quotes no list. |
| 2026-11-18 | validation | validated | Validated again after the notes were settled: AT-001 to AT-042 pass (278 passed, 15 skipped in the spec's file; the whole suite 2190 passed, 155 skipped, the documentation examples included); ruff, ruff format, mypy and `uv lock --check` pass. The README's 21 links resolve, and the amended criteria of `append_only_callback`, `scd2_callback`, `mongodb_reader` and `mysql_connectors` match the code and their tests. Not run: the 11 opt-in integration tests (15 cases), for lack of Spark and of a Snowflake account. One cosmetic note for the doc-keeper: the reworded line at `docs/concepts/connectors.md:2456` runs to about 160 columns in prose wrapped at 100. |
| 2026-11-18 | validation | tested | Returned to the doc-keeper to rewrap that paragraph, so that the commit lands without notes. |
| 2026-11-18 | documentation | documented | The doc-keeper rewrapped the "The key text" paragraph of the Snowflake section at 100 columns; the main session checked that the text is identical apart from whitespace. |
| 2026-11-18 | implementation | implemented | Run again after the rewrap: the implementer changed nothing; 2190 passed, 155 skipped; ruff and mypy pass. |
| 2026-11-18 | validation | validated | Validated again after the rewrap: AT-001 to AT-042 pass (278 passed, 15 skipped in the spec's file; the whole suite 2190 passed, 155 skipped, the documentation examples included); ruff, ruff format, mypy and `uv lock --check` pass. Not run: the 11 opt-in integration tests (15 cases), for lack of Spark and of a Snowflake account. One cosmetic note for the doc-keeper: 14 prose lines of the Snowflake section run to 101–104 columns; two lines of 119 are unbreakable code spans. |
| 2026-11-18 | validation | tested | Returned to the doc-keeper to rewrap every prose line of the Snowflake section past 100 columns. |
| 2026-11-18 | documentation | documented | The doc-keeper rewrapped eleven paragraphs and list items of the Snowflake section at 100 columns. The main session checked the file: the text is identical apart from whitespace, no code span is broken, and outside code blocks and tables the only lines past 100 are the two unbreakable code spans the validator named. |
| 2026-11-18 | implementation | implemented | Run again after the rewrap: the implementer changed nothing; 2190 passed, 155 skipped; ruff and mypy pass. |
| 2026-11-18 | validation | validated | Validated with no notes left: AT-001 to AT-042 pass (278 passed, 15 skipped in the spec's file; the whole suite 2190 passed, 155 skipped, the documentation examples included); ruff, ruff format, mypy and `uv lock --check` pass; in the Snowflake section only two unbreakable code spans run past 100 columns. Not run: the 11 opt-in integration tests of decision 32 (15 cases), for lack of Spark and of a Snowflake account; CI sets no `INGESTIO_SNOWFLAKE_*` variable, and the tier needs a process of its own. |
| 2026-11-18 | amendment | validated | Rebased onto `main` at v0.8.0, which holds [mysql_cdc](mysql_cdc.md); the main session resolved the conflicts at the maintainer's request. AT-037 amended as Lists of systems under Behaviour foresees: `mysql_cdc` landed first, so the source message lists it after `mysql`. What the criterion checks is unchanged; `SOURCE_NAMES` in `tests/test_snowflake_connectors.py` gains `mysql_cdc`, and the lists of `tests/test_mysql_cdc.py` gain `snowflake`. The conflicting lines of `docs/`, ADR 0012, ADR 0015, `README.md`, the callback table, `ReadSource`'s docstring, the specs `mongodb_reader` and `mysql_connectors` and their tests keep both systems. |
