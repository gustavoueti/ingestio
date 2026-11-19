# Spec: MySQL CDC reader of the binary log

- **Status:** validated
- **Slug:** `mysql_cdc`. The spec is `specs/mysql_cdc.md` and its tests are `tests/test_mysql_cdc.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md),
  [ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md),
  [ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md),
  [specs/mysql_connectors.md](mysql_connectors.md) (validated; this spec builds on it),
  [specs/config_connectors.md](config_connectors.md),
  [specs/mongodb_reader.md](mongodb_reader.md),
  [backlog idea 8](../docs/backlog.md#8-mysql-reader-and-writer-with-cdc),
  [Connectors](../docs/concepts/connectors.md),
  [Configuration](../docs/concepts/configuration.md),
  [Callbacks](../docs/concepts/callbacks.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

`MySQLCDCReader` reads the changes of one MySQL table from the server's binary log, on the Spark
driver with `python-mysql-replication`, and returns them as a Spark DataFrame of change events,
one row per changed row: the table's columns, typed as `MySQLReader` types them through JDBC, then
five metadata columns that carry the operation, the binary log file and offset of the
transaction's commit, the commit time and the order of the event within the read. The job passes
the position its last run reached as a new argument of `ReadSource`, `binlog_position`, and takes
the next position from the DataFrame, as it does with a watermark; `read()` reads up to the end of
the binary log that it captures when it starts. Without a position, the reader captures the
current position and returns a snapshot: the whole table read through Spark JDBC, each row an event
that carries the captured position. Applying the events to a target is a later callback.

The job names the system `mysql_cdc` in its configuration, a source only, with the keys
`table_name`, `server_id` and `options`; the `connection` argument is a `MySQLConnection`.
`python-mysql-replication` goes into a new extra, `ingestio[mysql-cdc]`.

The maintainer took seven decisions on 2026-10-24, before planning ([Decisions](#decisions) 1 to
7). The same day the maintainer answered the 21 open questions of the first draft, each with the
recommended option except question 10, the commit time, which took option (a) because the library
exposes no commit timestamp (V1), and accepted the proposed decisions 20 to 26 as written. The
answers are decisions 27 to 47; a rule or a criterion that rests on one of them names it. The main
session verified the facts V1 to V6 in the sources.

## Motivation

- **Backlog idea 8, second of three specs.** [specs/mysql_connectors.md](mysql_connectors.md) gave
  `MySQLConnection`, `MySQLReader`, `MySQLWriter` and the `mysql` system. This spec gives the change
  data capture reader; the callback that applies its events to a target comes after it, with a
  spec of its own (decision 1).
- **Deletes.** A watermark read returns the rows whose watermark grew and never sees a row that
  was deleted. The binary log records every insert, update and delete (decision 2).
- **Incremental by position.** The job already keeps the last watermark and passes it to
  `ReadSource` ([ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md):
  per-run values are callback arguments). A binary log position is kept and passed the same way
  (decision 3).
- **A frozen read.** The watermark readers freeze their slice with an upper bound, so that every
  action on the DataFrame sees the same rows and the next watermark never passes what the DataFrame
  holds. The CDC reader freezes its read at the end of the binary log captured when `read()` starts
  (decision 3).
- **A format for the apply.** The later callback upserts and deletes by key. One row per changed
  row, with the operation and an order, is the shape a `MERGE` or an apply-changes step expects,
  and an update that changes the primary key, split into a delete and an insert, leaves no old key
  behind in a target applied by key (decision 7).

## Fit with the philosophy

- **Concept: connector.** `MySQLCDCReader` satisfies `Reader[DataFrame]`, structurally, with no
  base class and no registration
  ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)). It talks to MySQL and returns
  what the binary log holds for one table; it decides no write strategy. Applying the events, CDC
  replication, is a callback, as the philosophy lists it and as
  [ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md) makes each write
  strategy (decision 1).
- **No third concept.** The position is a value that the job keeps and passes, a tuple
  (decision 29), as the watermark is. The metadata columns are data in the DataFrame.
  `MySQLConnection` is reused as it is.
- **The closed table.** `ReadSource` builds the reader from one more row of the table
  ([ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md)), which stays
  closed.
- **Essential by default; nothing silent.** The reader catches nothing to carry on. A purged
  position fails the read (decision 6), so does a server setting that would give events without
  column names or drop them (decision 33), a change of the table's columns that an event cannot
  follow (decision 37), a value the reader cannot convert (decision 38), and rows that no commit
  follows (decision 35).
- **Simplicity.** One class, in the module of the other MySQL connectors (decision 27). It reuses
  `MySQLConnection`'s private PyMySQL arguments (decision 15 of
  [specs/mysql_connectors.md](mysql_connectors.md), built for this reuse) and a `MySQLReader` for
  the table's schema and the snapshot (decision 40). No registry, no new protocol, no new callback.
- **Compose, do not impose.** The job keeps the position, in its own control table, and decides
  when to take a new snapshot by passing none. A job's own callback can still build a
  `MySQLCDCReader` itself.
- **No dependencies in the core.** `python-mysql-replication` and PyMySQL go into an extra
  (decision 42); `ingestio` and `ingestio.connector.all` still import without them.
- **Filtering in the source: an exception.**
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md) asks a reader to filter
  inside the source. The replication protocol sends every event of the binary log to the client,
  where the library filters the table's events (B7, G4, to be confirmed). The maintainer chose this
  mechanism (decision 2); the exception is recorded as an amendment of ADR 0013 (decision 45; see
  [ADRs](#adrs)).
- **Streaming.** The reader leans towards batch: one bounded read per call. A streaming CDC source
  that tracks its own offsets stays possible as another connector; see [ADRs](#adrs).

## Scope

In scope:

- `MySQLCDCReader`: the checks of the server's settings, the snapshot, the read of the binary log
  from a position up to a frozen bound, the event format, and the conversion of the values to the
  table's JDBC schema;
- the new `binlog_position` argument of `ReadSource`;
- the `mysql_cdc` source system, its keys, and an integer type for the key `server_id`;
- the `mysql-cdc` extra;
- the test doubles, and the opt-in integration tests against a real MySQL server.

Out of scope:

- **the callback that applies the events to a target**: the third spec of backlog idea 8
  (decision 1). This spec only makes the events serve it;
- GTID positions, and resuming after a failover to another server (decision 4);
- several tables in one reader (decision 32);
- continuous or blocking reads, heartbeats, and any streaming source;
- computing, persisting or advancing the position: the job's, as for the watermark;
- the row before an update (decision 7: an update carries the row after it);
- events for DDL statements, and following a change of the table's columns (decision 37);
- XA transactions that change the table (decision 35), MariaDB, MySQL 8.0 and earlier;
- changes to CI, `pyproject.toml`, `uv.lock` and `README.md`, which belong to the maintainer and
  the main session (see [Where it lives](#where-it-lives)).

## Public API

New, in the existing module `ingestio/connector/mysql.py` (decision 27), exported from
`ingestio.connector.all` only (not from `ingestio`):

```python
from ingestio.connector.all import MySQLCDCReader
```

```python
class MySQLCDCReader:
    """Reads the changes of one MySQL table from the binary log into a Spark DataFrame.

    Args:
        table_name: Source table, `table` or `database.table`. The configuration key of the
            same name.
        server_id: Server id the reader connects to the binary log with, unique among the
            replicas and readers of the server. The configuration key of the same name.
        connection: Where the table lives, with the credentials of a user who holds the
            replication privileges.
        binlog_position: The binary log position the last run reached, `(file, offset)`. Only
            the transactions committed after it are read. Without it, the reader returns a
            snapshot of the table.
        options: Spark JDBC options and Connector/J properties for the snapshot and for the
            table's schema, passed through as on `MySQLReader`.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
        server_id: int,
        connection: MySQLConnection,
        *,
        binlog_position: tuple[str, int] | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...

    def read(self) -> DataFrame:
        """Return a DataFrame of the table's change events, or its snapshot."""
```

- `DataFrame` and `SparkSession` are `pyspark.sql.DataFrame` and `pyspark.sql.SparkSession`;
  `Mapping` is `collections.abc.Mapping`; `MySQLConnection` is the class of
  [specs/mysql_connectors.md](mysql_connectors.md), unchanged.
- `table_name`, `server_id` and `connection` are required, so they are positional, with the keys
  before `connection`, as on `MongoDBReader(database, collection, connection)` (decisions 27 and
  31). Every other argument is keyword-only.
- The attributes of `MySQLCDCReader` are not documented, so they are not public API. Its `repr` is
  specified under [Building](#building).

**The columns of the DataFrame** are part of the contract: the table's columns, in the order and
with the types of the JDBC schema that `MySQLReader` gives, then these five, in this order
(decision 30):

| Column | Spark type | Nullable | Holds |
|---|---|---|---|
| `_ingestio_operation` | `string` | no | `insert`, `update`, `delete` or `snapshot` |
| `_ingestio_binlog_file` | `string` | no | the binary log file of the transaction's commit, such as `binlog.000042` |
| `_ingestio_binlog_offset` | `bigint` | no | the offset where the next event after the commit starts |
| `_ingestio_commit_time` | `timestamp` | yes | the header timestamp of the transaction's commit event, to the second (decision 36); `null` for a snapshot row |
| `_ingestio_sequence` | `bigint` | no | the order of the event within the read, from 1; `0` for every snapshot row |

**Changed: `ReadSource`** (decisions 28 and 29), exported by `ingestio` and `ingestio.callback.all`
as before. It gains one keyword-only argument, after `watermark_value`:

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
        binlog_position: tuple[str, int] | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...
```

Its docstring gains: `binlog_position: Binary log position the last run reached, (file, offset),
for the systems that take one.` `AppendOnly`, `FullLoad` and `SCD2` do not change.

**Changed: the configuration:**

- the system `mysql_cdc`, a source that builds `MySQLCDCReader`, and not a target;
- source keys: `table_name` and `server_id` (required), `options` (optional);
- `server_id` is a JSON integer, a new type of key (decision 31);
- callback arguments: `connection`, a `MySQLConnection`, required; `binlog_position` and `spark`
  optional; `client` and `watermark_value` not taken;
- `binlog_position` given to any other system raises, like any argument a system does not take;
- the list of source systems in the message `<path>: source.system '<name>' is not one of: <names>`
  gains `mysql_cdc`.

**New installable extra:** `ingestio[mysql-cdc]` (decision 42).

**Unchanged:** `Reader`, `Writer`, `MySQLConnection`, `MySQLReader`, `MySQLWriter`, every other
connector, the `mysql` extra, `AppendOnly`, `FullLoad` and `SCD2`.

### Usage sketch

A sketch of a Databricks job, not a runnable example: `dbutils`, `last_position()` and
`save_position()` are the job's. Until the apply callback exists, the job appends the events to a
table of changes. The configuration, `orders_cdc.json`:

```json
{
  "source": {"system": "mysql_cdc", "table_name": "shop.orders", "server_id": 4001},
  "target": {"system": "unity_catalog", "table_name": "main.bronze.orders_changes"}
}
```

```python
from pyspark.sql import functions as F

from ingestio import AppendOnly, Process, ReadSource, Step
from ingestio.connector.all import MySQLConnection

shop = MySQLConnection(
    "mysql.internal",
    "shop",
    dbutils.secrets.get("mysql", "cdc_user"),  # the job reads its secrets; the library never does
    dbutils.secrets.get("mysql", "cdc_password"),
    ssl_mode="VERIFY_IDENTITY",
)
position = last_position()  # None on the first run, which takes a snapshot

process = Process([
    Step("extract", [ReadSource("orders_cdc.json", into="changes", connection=shop,
                                binlog_position=position)]),
    Step("load", [AppendOnly("orders_cdc.json", key="changes")]),
]).run()

# The next position: the commit of the last event. A read with no event keeps the old one.
events = process.state["changes"]
if position is not None:  # the rows of a snapshot all carry the same position
    events = events.orderBy(F.col("_ingestio_sequence").desc())
last = events.select("_ingestio_binlog_file", "_ingestio_binlog_offset").first()
if last is not None:
    save_position((last[0], last[1]))
```

Each action on a snapshot's DataFrame reads the table again, this `first()` included (see
[Snapshot](#snapshot)).

## Behaviour

### Evidence

The main session read `python-mysql-replication` at tag 1.0.17 (the latest, released 2026-08-06,
Apache-2.0) on 2026-10-24 and gave the planner the facts B1 to B14. It gave R1 to R6 from its own
knowledge of the MySQL server, **to be confirmed**. When the maintainer answered the open
questions, the same day, the main session verified V1 to V6 in the sources: they refute G2 and
confirm G6, G10 and G15. The other G facts are the planner's own knowledge, **not verified**: each
is to be confirmed in the source or the manual it names, or by the integration tests of this spec,
and is cited as "(G*n*, to be confirmed)" where a rule rests on it. The facts of
[specs/mysql_connectors.md](mysql_connectors.md) are cited with that spec's name, for instance
"mysql_connectors S3"; its K facts stay unconfirmed.

**`python-mysql-replication`** (PyPI `mysql-replication`; files under `pymysqlreplication/`):

- **B1** (`setup.py`): `install_requires = ["packaging", "pymysql>=1.1.0"]`. The README: MySQL
  8.0.14 or later (from v1.0), Python 3.10 to 3.14, and `binlog_row_metadata=FULL` and
  `binlog_row_image=FULL` must be set.
- **B2** (`binlogstream.py`): `BinLogStreamReader(connection_settings, server_id,
  ctl_connection_settings=None, resume_stream=False, blocking=False, only_events=None,
  log_file=None, log_pos=None, end_log_pos=None, filter_non_implemented_events=True,
  ignored_events=None, auto_position=None, only_tables=None, ignored_tables=None,
  only_schemas=None, ignored_schemas=None, freeze_schema=False, skip_to_timestamp=None,
  report_slave=None, slave_uuid=None, pymysql_wrapper=None, slave_heartbeat=None,
  is_mariadb=False, annotate_rows_event=False, ignore_decode_errors=False, verify_checksum=False,
  enable_logging=True, use_column_name_cache=False)`. `server_id` has no default.
- **B3**: `blocking=False` sets `BINLOG_DUMP_NON_BLOCK`. The server sends an EOF at the end of the
  binary log, and `fetchone()` closes the stream and returns `None`. That end is where the server is
  when it gets there, not a bound fixed in advance: writes that keep arriving keep being sent.
- **B4**: with `log_file` and `log_pos` both given and `resume_stream=True`, the dump starts at
  `log_pos`; with `resume_stream=False`, at offset 4 of `log_file`. With either of them `None`, and
  no `auto_position`, the library takes the current position from `SHOW BINARY LOG STATUS`, falling
  back to `SHOW MASTER STATUS` on a parse error, from 1.0.16 only; 1.0.15 and earlier run
  `SHOW MASTER STATUS`, which MySQL 8.4 removed.
- **B5**: `end_log_pos` is compared with the current offset only (`self.log_pos >=
  self.end_log_pos`), not with the file, so a bound in another file than the current one stops at
  the wrong place. A `RotateEvent` updates `log_file` and `log_pos` and clears the table map.
- **B6**: each event's `packet.log_pos` is the offset where the next event starts, and the stream
  updates `self.log_pos` from it. `XidEvent` (with `xid`) marks an InnoDB commit; `QueryEvent`
  carries `BEGIN` and DDL; `GtidEvent` precedes each transaction when GTIDs are on. Resuming at an
  offset inside a transaction skips the `TABLE_MAP_EVENT` that its row events need, so a resume
  must start at a transaction boundary, such as the end of an `XidEvent`.
- **B7** (`row_event.py`, `RowsEvent.__init__`): `only_tables` is matched against the table name
  alone and `only_schemas` against the database alone; a row event filtered out is not processed.
- **B8** (`row_event.py`): `WriteRowsEvent.rows[i]["values"]`, `DeleteRowsEvent.rows[i]["values"]`,
  `UpdateRowsEvent.rows[i]["before_values"]` and `["after_values"]`, each a dict by column name.
  The column names and the primary key come from the optional metadata of the `TABLE_MAP_EVENT`
  (`simple_primary_key_list`, `primary_keys_with_prefix`) when `binlog_row_metadata=FULL`;
  `event.primary_key` is `""`, a name, or a tuple of names. `event.schema` and `event.table` name
  the table; `event.timestamp` is the event header's timestamp, in seconds.
- **B9** (`binlogstream.py`, `__check_optional_meta_data`): when `binlog_row_metadata` is not
  `FULL`, the library logs a warning and goes on; it does not fail, and the column names are then
  missing.
- **B10** (`row_event.py`, decoded values): `TIMESTAMP` and `TIMESTAMP2` with
  `datetime.datetime.utcfromtimestamp(...)`, a naive datetime in UTC (`utcfromtimestamp` is
  deprecated since Python 3.12 and emits a `DeprecationWarning`); `DATETIME` naive, as stored;
  `NEWDECIMAL` as `Decimal`; `TIME` as `datetime.timedelta`; `YEAR` as an `int` (+1900); `ENUM` as
  its string; `SET` as a Python `set` of strings, and an empty `SET` as `None`, the same as `NULL`;
  `BIT` as a string of `0` and `1` characters; `JSON` decoded from MySQL's binary JSON into a Python
  value; `GEOMETRY` as bytes; strings decoded with the column's character set when it is known.
  These differ from the types Spark's JDBC source gives the same columns (mysql_connectors S3;
  Connector/J maps `YEAR` to a date by default and `TIME` to a timestamp).
- **B11** (`binlogstream.py`): errors of the stream other than 2013 and 2006, on which the library
  reconnects and continues, propagate from `fetchone()` as `pymysql` errors. A purged position is
  reported by the server as an error of the dump (R6).
- **B12**: the control connection copies `connection_settings` and sets `db="information_schema"`,
  `cursorclass=DictCursor` and `autocommit=True`; PyMySQL v1.2.3 (`connections.py`, lines 221 to
  223) ignores `db` when `database` is given, so with `MySQLConnection`'s `database` the control
  connection would open on the job's database. The stream and the control connection are opened
  with `pymysql_wrapper` when it is given, `pymysql.connect` otherwise.
- **B13**: the library logs through its own `logging` loggers; `enable_logging=False` turns off part
  of it (V4 says which part).
- **B14**: Connector/J's `useCursorFetch` and the other read defaults of
  [specs/mysql_connectors.md](mysql_connectors.md) apply to the snapshot's JDBC read.

**Verified by the main session on 2026-10-24**, when the maintainer answered the open questions:

- **V1** (refutes G2; `pymysqlreplication/event.py` and `packet.py` at 1.0.17): `GtidEvent.__init__`
  reads only `commit_flag`, `sid`, `gno`, `lt_type` and, from MySQL 5.7, `last_committed` and
  `sequence_number`; no `immediate_commit_timestamp` is parsed. `packet.py` maps
  `ANONYMOUS_GTID_LOG_EVENT` to `event.NotImplementedEvent`, which
  `filter_non_implemented_events=True` drops, so with `gtid_mode=OFF` no GTID event reaches the
  reader at all.
- **V2** (confirms G6; 1.0.17): `constants/BINLOG.py` and the event map of `packet.py` have no
  `TRANSACTION_PAYLOAD_EVENT`, and an unknown type becomes `NotImplementedEvent` (`packet.py`, line
  116, `self.__event_map.get(self.event_type, event.NotImplementedEvent)`): with
  `binlog_transaction_compression=ON`, a compressed transaction's rows would be dropped without an
  error. `PARTIAL_UPDATE_ROWS_EVENT` maps to `PartialUpdateRowsEvent`, but a partial JSON value is
  not rebuilt: `row_event.py`, around line 363, marks it `NONE_SOURCE.JSON_PARTIAL_UPDATE`.
- **V3** (confirms G10; 1.0.17): `QueryEvent.query` is set in `event.py`, line 573, as
  `query.decode("utf-8", errors="backslashreplace")`. Without column names, `row_event.py`, lines
  210 to 225, fetches them from `INFORMATION_SCHEMA` only when `use_column_name_cache=True`;
  otherwise the key is `"UNKNOWN_COL" + str(i)`. `RowsEvent.rows` is a lazy `@property`
  (`row_event.py`, lines 641 to 645), which fetches the rows on first access.
- **V4** (`binlogstream.py` at 1.0.17): `enable_logging=False` skips `__log_valid_parameters()`
  in `BinLogStreamReader.__init__` (lines 284 and 285), the `logger.info(comment)` of line 801
  included, which sits inside that method (lines 786 to 801), and the logs of the column-name
  cache, which `enable_logging` governs too. Three calls stay unconditional: the two
  `logger.warning` calls of `__check_optional_meta_data` (lines 581 and 589) and the reconnect
  warning (line 619).
- **V5**: `pymysqlreplication/` at 1.0.17 has no `py.typed` marker, and typeshed has no stubs for
  it: its `stubs/` has `PyMySQL` and `mysqlclient` only.
- **V6** (confirms G15; pyspark 4.2.0, `pyspark/sql/types.py`): `TimestampType.toInternal` turns an
  aware datetime into `calendar.timegm(dt.utctimetuple())` and a naive one into
  `time.mktime(dt.timetuple())`, in the process's local time zone. `SparkSession.createDataFrame`
  has `verifySchema: bool = True`.

**MySQL server, the main session's knowledge, to be confirmed:**

- **R1**: `SHOW BINARY LOGS` lists the binary log files the server still has, oldest first, with
  their sizes (columns `Log_name`, `File_size`, `Encrypted`); purged files are not listed.
- **R2**: `SHOW BINARY LOG STATUS` needs `REPLICATION CLIENT`; its first two columns are `File` and
  `Position`.
- **R3**: the dump needs `REPLICATION SLAVE`.
- **R4**: a replica's `server_id` must be unique among the clients connected to the source; a
  duplicate disconnects the other.
- **R5**: binary log file names end in a numeric suffix (`binlog.000123`) that grows past six
  digits only after 999999 files, so they are ordered by that number, not by their text.
- **R6**: a dump from a purged position fails with MySQL error 1236.

**The planner's knowledge that V1 to V6 settle:**

- **G2**, refuted by V1: since MySQL 8.0.1 every transaction's GTID event carries
  `immediate_commit_timestamp`, in microseconds since the epoch, UTC, and the server writes an
  anonymous GTID event when `gtid_mode=OFF`; the library's `GtidEvent` exposes the value under that
  name, and returns the anonymous GTID event as a `GtidEvent`. The library parses no such value and
  drops the anonymous GTID event (V1).
- **G6**, confirmed by V2: with `binlog_transaction_compression=ON` a transaction's events travel in
  one `Transaction_payload` event; with `binlog_row_value_options=PARTIAL_JSON` a partial update of
  a JSON document is logged as a diff. The library reads neither (V2).
- **G10**, confirmed by V3: `QueryEvent.query` holds the statement's text; without
  `binlog_row_metadata=FULL` the library's row dicts have placeholder keys instead of column names,
  `UNKNOWN_COL<i>` (V3).
- **G15**, confirmed by V6: PySpark's `createDataFrame` with a `StructType` checks the Python type
  of each value (`verifySchema`), turns an aware `datetime` into its instant for a `timestamp`
  field, and reads a naive one in the Python process's local time zone.

**The planner's knowledge, to be confirmed:**

- **G1**: the server writes a transaction to the binary log as a whole, at its commit, so the
  `Position` of `SHOW BINARY LOG STATUS` is always a transaction boundary. A transaction on InnoDB
  ends with an `XidEvent`; on a non-transactional engine, with a `QueryEvent` whose statement is
  `COMMIT`; a DDL statement is a `QueryEvent` of its own (Reference Manual, "The Binary Log").
- **G3**: an event header's timestamp is the start of the statement that wrote it, so an
  `XidEvent`'s is the start of the statement that committed: for an autocommitted statement, its
  start, not its commit.
- **G4**: the dump protocol has no filter by table: the server sends every event of the binary log,
  and `only_tables` and `only_schemas` filter on the client (B7).
- **G5**: an XA transaction is logged with `XA START` and `XA END` query events and an XA prepare
  event, and committed later by an `XA COMMIT` query event; a transaction that changed a
  non-transactional table and rolls back ends with `ROLLBACK`.
- **G7**: `binlog_format`, `binlog_row_image`, `binlog_row_value_options` and
  `binlog_transaction_compression` also have a session scope, which a session with the privilege
  can change; a check of the global values does not see it.
- **G8**: since MySQL 8.0.23, `SELECT *` leaves invisible columns out, a generated invisible primary
  key included, while row events of the binary log carry them.
- **G9**: under `lower_case_table_names=1` table names are stored in lowercase, and the library
  compares names exactly (B7).
- **G11**: `SELECT @@GLOBAL.<name>` reads each of the variables of decision 33; `log_bin` and
  `binlog_transaction_compression` read as `1` or `0`; `binlog_format` still exists in 8.4 and 9.x,
  deprecated since 8.0.34.
- **G12**: MySQL 8.4's default `binlog_row_metadata` is `MINIMAL`.
- **G13**: `SHOW BINARY LOG STATUS` returns no row when binary logging is off.
- **G14**: Connector/J returns a `BIT(n)` as bytes, big-endian, `ceil(n/8)` of them; a `TINYINT(1)`
  as a boolean by default (`tinyInt1isBit`); a `SET` as its members joined with `,` in the
  column's definition order; a `JSON` value as MySQL's text of it.

### Building

`MySQLCDCReader` validates its arguments when it is built, in this order; the first that fails
raises `ValueError`:

| Argument | Rule | Message |
|---|---|---|
| `table_name` | one or two non-empty parts, split on `.` (mysql_connectors decision 28) | `table_name must be 'table' or 'database.table'` |
| `server_id` | an `int`, not a `bool`, from 1 to 4294967295 (decision 31) | `server_id must be an integer between 1 and 4294967295` |
| `binlog_position` | `None`, or a `tuple` of exactly two items (decision 29) | `binlog_position must be a (file, offset) tuple` |
| its file | a `str` that matches `.+\.[0-9]+` as a whole (R5) | `binlog_position file must be a binary log name that ends in a number, such as 'binlog.000123'` |
| its offset | an `int`, not a `bool`, of at least 4 (B4) | `binlog_position offset must be an integer of at least 4` |
| `options` | no reserved key, as on `MySQLReader` (mysql_connectors decision 32) | `option '<key>' is set by the connector` |

- The reserved keys are `MySQLReader`'s: `url`, `dbtable`, `query`, `user`, `password`, `driver`,
  `sslMode`, `useSSL`, `requireSSL` and `verifyServerCertificate`, in any case, `<key>` as given.
- After these checks, building imports `pymysql` and `pymysqlreplication`. When either import
  fails, it raises `ImportError("MySQLCDCReader needs mysql-replication: pip install
  'ingestio[mysql-cdc]'")`, chained to the original error (decision 42). The argument errors come
  first, so they do not depend on the install, as on `S3Reader`.
- Building does nothing else: no Spark session, no network, no PyMySQL connection, no stream.
- `repr` and `str` show what identifies the reader on the server, and never the password:
  `MySQLCDCReader(table_name='shop.orders', server_id=4001, host='db.internal', database='shop')`.
  `binlog_position` and `options` are not shown.

### The position

- A position is `(file, offset)`: the name of a binary log file and an offset in it (decision 4),
  a `tuple[str, int]` (decision 29). The position a job passes is one the reader returned, the
  `_ingestio_binlog_file` and `_ingestio_binlog_offset` of an event: the end of a transaction's
  commit event, where the next transaction starts (B6), so that a read resumes at a transaction
  boundary.
- **Order.** Two positions are compared by the number that ends the file name, then by the offset
  (R5): `("binlog.1000000", 4)` comes after `("binlog.999999", 9000)`. The *number* of a file name
  is `int(name.rsplit(".", 1)[1])`.
- **The next position** is the one of the event whose `_ingestio_sequence` is the highest, as the
  [usage sketch](#usage-sketch) shows: events are numbered in the order of the binary log
  (decision 30). A snapshot's rows all carry the captured position and the sequence 0, so any of
  them gives it.
- A read that returns no event gives no position: the job keeps the one it passed (decision 3).

### `read()`

Each call runs these steps, in this order:

1. **Session.** The reader uses `spark` when it is given, and otherwise calls
   `pyspark.sql.SparkSession.active()`, at this step, never when it is built. It never creates a
   session; when none is active, PySpark's error propagates.
2. **Server checks and the bound**, on one PyMySQL connection: see
   [Server checks](#server-checks). They give the *bound*, the position at the end of the binary
   log now.
3. **The table's schema**: see [The table's schema](#the-tables-schema).
4. Without `binlog_position`, the **snapshot**: see [Snapshot](#snapshot). With it, the **read of
   the binary log** from the position up to the bound: see
   [Reading the binary log](#reading-the-binary-log).

The position is captured in step 2, before the table's schema is read and before any action of
the job copies a row, so that no change made after it is lost (decision 5).

### Server checks

The reader opens one connection, `pymysql.connect(**connection._connect_arguments())`, the very
arguments of [specs/mysql_connectors.md](mysql_connectors.md) (its decisions 15, 16 and 29), and
executes, through a cursor, with `cursor.execute(<statement>)` and no arguments (mysql_connectors
decision 17):

1. `SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image, @@GLOBAL.binlog_row_metadata, @@GLOBAL.binlog_row_value_options, @@GLOBAL.binlog_transaction_compression`,
   then `fetchone()` (decision 33, G11). Each value, as `str(value)` compared ignoring case, must
   be the required one below; the first that is not, in this order, raises `ValueError`:

   | Variable | Required | Message |
   |---|---|---|
   | `log_bin` | `1` | `the server's log_bin is '<value>'; MySQLCDCReader needs '1'` |
   | `binlog_format` | `ROW` | `the server's binlog_format is '<value>'; MySQLCDCReader needs 'ROW'` |
   | `binlog_row_image` | `FULL` | `the server's binlog_row_image is '<value>'; MySQLCDCReader needs 'FULL'` |
   | `binlog_row_metadata` | `FULL` | `the server's binlog_row_metadata is '<value>'; MySQLCDCReader needs 'FULL'` |
   | `binlog_row_value_options` | the empty text | `the server's binlog_row_value_options is '<value>'; MySQLCDCReader needs ''` |
   | `binlog_transaction_compression` | `0` | `the server's binlog_transaction_compression is '<value>'; MySQLCDCReader needs '0'` |

   `<value>` is `str(value)` as the server returned it.
2. With `binlog_position` only: `SHOW BINARY LOGS`, then `fetchall()`. Each row's first item is a
   file name and its second the file's size in bytes (R1). The position given is checked against
   them (decisions 6 and 39), and the first case that holds raises `ValueError`:

   | Case | Message |
   |---|---|
   | its file is listed, and its offset is above the file's size | `binlog_position <position> is past the end of '<file>', which is <size> bytes long` |
   | its file is not listed; the oldest listed file has the same name before its last `.`, and a higher number | `binlog_position <position> has been purged from the server; its oldest binary log is '<oldest>'` |
   | its file is not listed, in any other case | `binlog_position <position> names a binary log the server does not have; its binary logs are '<oldest>' to '<newest>'` |

   `<position>` is `repr((file, offset))`, such as `('binlog.000040', 4)`; `<oldest>` and
   `<newest>` are the first and the last file listed. An offset equal to the size is accepted: it
   is the end of the file. An empty list cannot reach these cases: the `log_bin` check runs first,
   and a server with binary logging on lists at least its current file (R1, G13, to be confirmed).
   If the list is empty all the same, the position check passes.
3. `SHOW BINARY LOG STATUS`, then `fetchone()`. The row's first two items are the bound's file and
   offset (R2), the end of the binary log now (decision 3), which is a transaction boundary (G1).

- The reader closes the connection with `close()` once the last statement has run, and in every
  case; it never calls `begin()`, `commit()` or `rollback()`. When a statement fails, its error
  propagates, the same object; when `close()` fails after another error, the original error
  propagates with one note, `the control connection could not be closed: <type>: <message>`
  (mysql_connectors decision 20).
- The checks run on every read, the snapshot's included, so that a job does not copy a table that
  it cannot follow afterwards (decision 33). When a check fails, nothing after it runs: no other
  statement, no Spark load, no stream.
- `SHOW BINARY LOG STATUS` returns no row only when binary logging is off (G13), which the first
  check rejects.

### The table's schema

- The reader builds `MySQLReader(table_name, connection, options=options, spark=<the session>)`
  with its own `table_name`, `connection` and `options`, and calls its `read()`: a full read, one
  JDBC load with exactly the options of [specs/mysql_connectors.md](mysql_connectors.md), its
  read defaults and its UTC sessions included (its decisions 30 and 35; B14). The reader keeps the
  DataFrame it returns, the *table frame*, and reads its `schema` once: Spark's own schema query,
  on the driver. It never runs an action on the table frame for a read of the binary log
  (decision 40).
- **Metadata names.** When a field of the schema has the name of one of the five metadata columns,
  compared ignoring case, the read raises
  `ValueError("the table already has a column '<name>', which MySQLCDCReader adds to every event")`,
  with `<name>` as the schema writes it, before the snapshot or the stream (decision 30).
- The table's columns of every event are the schema's fields, in their order, with their names,
  types and nullability; the five metadata columns follow (decision 7).

### Snapshot

Without `binlog_position` (decisions 5 and 40), `read()` returns
`<table frame>.selectExpr("*", <e1>, <e2>, <e3>, <e4>, <e5>)`, with exactly these expressions,
where `<file>` and `<offset>` are the bound's, captured in step 3 of the server checks:

| | Expression |
|---|---|
| `<e1>` | `'snapshot' AS _ingestio_operation` |
| `<e2>` | `L(<file>) AS _ingestio_binlog_file` |
| `<e3>` | `CAST(<offset> AS BIGINT) AS _ingestio_binlog_offset` |
| `<e4>` | `CAST(NULL AS TIMESTAMP) AS _ingestio_commit_time` |
| `<e5>` | `CAST(0 AS BIGINT) AS _ingestio_sequence` |

- `L(t)` is a Spark SQL string literal: `'`, then `t` with each `\` written `\\` and each `'`
  written `\'`, then `'`, as `UnityCatalogReader` writes its string literals. `<offset>` is written
  with `str()`.
- The snapshot is the table frame's lazy plan: the rows are copied when the job runs an action, and
  every action reads the table again, as a full read of `MySQLReader` does. Every row carries the
  captured position, so the next run starts from it (decision 5). Changes committed between the
  capture and the copy are in the copy and are read again by the next run, which an apply that
  upserts and deletes by key tolerates (decision 5).
- No stream is opened and `createDataFrame` is not called.

### Reading the binary log

With `binlog_position` `(file, offset)` and the bound `(bound_file, bound_offset)`:

- **Nothing to read.** When the position is at or after the bound, in the order of
  [The position](#the-position), the reader opens no stream, and the events are none.
- **The stream.** Otherwise it calls `pymysqlreplication.BinLogStreamReader`, looked up as an
  attribute of the `pymysqlreplication` module when `read()` runs, with exactly these keyword
  arguments and no positional one:

  | Argument | Value |
  |---|---|
  | `connection_settings` | `connection._connect_arguments()` without its `database` item (decision 41, B12) |
  | `server_id` | the reader's `server_id` (decision 31) |
  | `resume_stream` | `True` (B4) |
  | `log_file`, `log_pos` | the position's file and offset (B4) |
  | `blocking` | `False` (B3) |
  | `only_events` | a list of the library's `WriteRowsEvent`, `UpdateRowsEvent` and `DeleteRowsEvent` (`pymysqlreplication.row_event`) and `XidEvent` and `QueryEvent` (`pymysqlreplication.event`), and no other class (decisions 35 and 36) |
  | `only_schemas` | `[<database>]`: the table's database part, or the connection's `database` when `table_name` has none |
  | `only_tables` | `[<table>]`: the table's name part (B7) |
  | `enable_logging` | `False` (decision 44, B13, V4) |

  Every other argument keeps the library's default.
- **The loop.** The reader calls `fetchone()` until it returns `None` (B3), handles each event as
  [Events](#events) says, and after each one compares the stream's position, its `log_file` and
  `log_pos` attributes right after that `fetchone()` (B5, B6), with the bound: when it is at or
  after the bound, the reader stops and calls `fetchone()` no more (decision 34). Events after the
  bound are never read, so the read is frozen at the bound whatever the server writes meanwhile
  (decision 3).
- The reader calls `close()` on the stream once, when it stops, in every case. When it stops on an
  error and `close()` fails too, the original error propagates with one note,
  `the binary log stream could not be closed: <type>: <message>`. A failed `close()` after a
  successful loop propagates.
- **The DataFrame.** After the stream is closed, `read()` returns
  `spark.createDataFrame(<rows>, <schema>)`, called once: `<rows>` is a `list` of tuples, one per
  event, the table's values in the schema's order, then the five metadata values; `<schema>` is a
  `StructType` of the table schema's fields, then these fields:
  `StructField("_ingestio_operation", StringType(), False)`,
  `StructField("_ingestio_binlog_file", StringType(), False)`,
  `StructField("_ingestio_binlog_offset", LongType(), False)`,
  `StructField("_ingestio_commit_time", TimestampType(), True)` and
  `StructField("_ingestio_sequence", LongType(), False)`. With no events, `<rows>` is `[]`.
- The events pass through the driver's memory before they become a DataFrame (decision 2). The
  DataFrame is built from them on the driver, so every action on it sees the same events, and none
  reads MySQL again.

### Events

The reader keeps a *buffer* of the current transaction's rows, empty at the start. For each event
that `fetchone()` returns (decision 35):

- **`WriteRowsEvent`**: for each item of `rows`, in order, one row, operation `insert`, from its
  `values`.
- **`DeleteRowsEvent`**: for each item of `rows`, one row, operation `delete`, from its `values`:
  the row before the delete (decision 7).
- **`UpdateRowsEvent`**: for each item of `rows`, from its `before_values` and its `after_values`:
  - the *key columns* are the event's `primary_key`: none when it is `""`, the one name when it is a
    `str`, the names when it is a tuple (B8);
  - when there are key columns and the value of at least one of them in `before_values` differs
    from its value in `after_values` (compared with `!=`, before any conversion), two rows: a
    `delete` from `before_values`, then an `insert` from `after_values` (decision 7);
  - otherwise one row, operation `update`, from `after_values` (decision 7).
- Every row of these three events goes into the buffer, in order, with its values matched to the
  schema's columns as *Columns of a row* below says, and converted as [Values](#values) says.
- **`XidEvent`**, or **`QueryEvent`** whose `query`, stripped of surrounding white space and
  compared ignoring case, is `COMMIT` (G1): the commit. Each row of the buffer, in order, becomes
  an event: its operation, the stream's `log_file` and `log_pos` right after this `fetchone()`, the
  *commit time*, and the next number of the read's sequence, which starts at 1. The commit time is
  the commit event's `timestamp`, the header's timestamp in seconds (B8), as an aware datetime,
  `datetime.fromtimestamp(timestamp, UTC)` (decision 36): the start of the statement that committed
  (G3, to be confirmed). The buffer is then emptied.
- **Any other `QueryEvent`** (`BEGIN`, DDL, `ROLLBACK`, `XA END`, ...): nothing, while the buffer
  is empty. When the buffer holds rows, the read raises
  `ValueError("the binary log has rows of the table at <position> that no commit follows")`, where
  `<position>` is `repr((log_file, log_pos))` of the stream right after this `fetchone()` (G5,
  decision 35).
- When the loop stops, at the bound or at the end of the stream, rows left in the buffer are
  dropped: their transaction had not committed within the bound, and the next run, which starts at
  or before it, reads it (decision 35).

The reader reads no attribute of an event other than `rows` and `primary_key` of a row event,
`query` of a `QueryEvent`, and `timestamp` of a commit event, an `XidEvent` or a `QueryEvent`
`COMMIT`; and of the stream, only `log_file` and `log_pos`.

**Columns of a row** (decision 37). A row's values are a dict by column name (B8). For each field of
the table's schema, in order, the value is the one whose key equals the field's name, compared
ignoring case. When no key matches a field, the read raises
`ValueError("the event of the table at <position> has no column '<name>'; the table's columns changed since it was written")`,
where `<name>` is the field's name and `<position>` as above. Keys that match no field are left out:
an invisible column (G8), a column dropped since the event. The same check catches events written
without column names, whose keys are `UNKNOWN_COL<i>` (B9, V3).

### Values

Each value is converted for the type of its field in the table's schema, by the first row of this
table that matches the field's Spark type and the value's Python type (decision 38); `None` stays
`None` for every type. *Bits* is a `str` that matches `[01]+` as a whole (a `BIT` value, B10).

| Field's Spark type | Value from the binary log (B10) | Value in the DataFrame |
|---|---|---|
| `BooleanType` | `bool` | the value |
| `BooleanType` | `int` (a `TINYINT(1)`, G14) | `value != 0` |
| `BooleanType` | bits (a `BIT(1)`) | `int(value, 2) != 0` |
| `ByteType`, `ShortType`, `IntegerType`, `LongType` | `int`, not `bool` | the value |
| `ByteType`, `ShortType`, `IntegerType`, `LongType` | bits (a `BIT(n)` on Spark 3.5, mysql_connectors S3) | `int(value, 2)` |
| `DecimalType` | `Decimal` | the value |
| `DecimalType` | `int`, not `bool` (a `BIGINT UNSIGNED`) | `Decimal(value)` |
| `FloatType`, `DoubleType` | `float` | the value |
| `StringType` | `str` | the value |
| `StringType` | `set` (a `SET`) | its members, sorted, joined with `,` |
| `StringType` | `dict`, `list`, `bool`, `int` or `float` (a `JSON` value) | `json.dumps(value, ensure_ascii=False)` |
| `BinaryType` | `bytes` | the value |
| `BinaryType` | bits (a `BIT(n)` on Spark 4, mysql_connectors S3) | `int(value, 2).to_bytes((len(value) + 7) // 8, "big")` (G14) |
| `DateType` | `date`, not `datetime` | the value |
| `DateType` | `int`, not `bool` (a `YEAR`) | `date(value, 1, 1)` |
| `TimestampType` | naive `datetime` (`TIMESTAMP` in UTC, `DATETIME` as stored) | `value.replace(tzinfo=UTC)` |
| `TimestampType` | aware `datetime` | `value.astimezone(UTC)` |
| `TimestampType` | `timedelta` (a `TIME`) | `datetime(1970, 1, 1, tzinfo=UTC) + value` |
| `TimestampNTZType` | naive `datetime` | the value |
| `TimestampNTZType` | `timedelta` (a `TIME`) | `datetime(1970, 1, 1) + value` |

- Any other pair raises
  `ValueError("column '<name>' of the event at <position>: cannot turn a <python type> into <spark type>")`,
  where `<name>` is the field's name, `<python type>` is `type(value).__name__`, `<spark type>` is
  the field type's `simpleString()`, and `<position>` as in [Events](#events). The message holds no
  value.
- `UTC` is `datetime.UTC`. PySpark reads a naive `datetime` for a `timestamp` field in the Python
  process's local zone and an aware one as its instant (V6), so every `datetime` for a
  `TimestampType` field is given the UTC zone before `createDataFrame`. A `TIMESTAMP` decoded by the
  library is a naive datetime in UTC (B10), so its instant is exact. A `DATETIME` is compared on the
  UTC wall clock, as the JDBC read in UTC sessions gives it (mysql_connectors decision 35), when the
  JVM's zone is UTC; under another JVM zone the JDBC side is not established (mysql_connectors,
  Known limitations).
- Where the value differs from what the JDBC read of the same row gives, the difference is a known
  limitation, listed below; the integration tests of tier 2 compare them.

### Errors

- The reader catches nothing to carry on: every error of PySpark, PyMySQL and
  `python-mysql-replication` propagates out of `read()`, the same object (B11), apart from the two
  notes on a failed `close()`. A purge between the check and the dump gives the server's error of
  the dump (R6), unchanged.
- No message or note holds a value of the table, a password, or anything of the connection but
  what its `repr` shows.
- The reader writes no log lines.

### In a configuration

The `mysql_cdc` system (decision 27) is one more row of the closed table, a source only:

| `system` | `source` builds | `target` builds | Extra the connector may need |
|---|---|---|---|
| `mysql_cdc` | `MySQLCDCReader` | not a target | `mysql-cdc` |

| `system` | Section | Required keys | Optional keys |
|---|---|---|---|
| `mysql_cdc` | source | `table_name`, `server_id` | `options` |

| `system` | Section | `connection` | `client` | `watermark_value` | `binlog_position` | `spark` |
|---|---|---|---|---|---|---|
| `mysql_cdc` | source | **required**: a `MySQLConnection` | no | no | optional | optional |
| every other system | source | as before | as before | as before | no | as before |

- **Keys.** Each key is the constructor argument of the same name, passed as it is: `table_name` a
  string, `options` an object of strings, numbers and booleans, as for `mysql`.
- **`server_id`** is a JSON integer (decision 31): a number that Python's `json` parses as an `int`,
  so written without a fraction or an exponent; `true` and `false` are not integers. Any other
  value, `null` included, raises `<path>: source.server_id must be an integer`, in the step of the
  JSON types. The range is the connector's: `"server_id": 0` raises the connector's `ValueError`,
  with the note.
- **Not keys**: the host, the database of the connection, the credentials, the port, `ssl_mode`,
  `watermark_column`, `binlog_position` and every other name the system does not take raise
  `<path>: source.<key> is not a key of system 'mysql_cdc'`.
- **`binlog_position`** is a callback argument of `ReadSource`, checked in the step of the
  callback arguments, after `watermark_value` and before `spark`. The callback hands the very
  object to the connector, which validates it. Given, and not `None`, to any other system, it
  raises `<path>: source system '<system>' takes no argument binlog_position`.
- **`connection`.** Missing: `<path>: source system 'mysql_cdc' needs the argument connection`.
  Not an instance of `MySQLConnection`, a subclass accepted:
  `<path>: source system 'mysql_cdc' needs a MySQLConnection as the argument connection, not <type name>`
  ([specs/mongodb_reader.md](mongodb_reader.md), decision 10).
- **Lists of systems.** `<names>` for a source becomes
  `bigquery, disk, gcs, json_api, mongodb, mysql, mysql_cdc, postgres, s3, unity_catalog, volume`.
  The target list and `SCD2`'s narrowed list do not change, so `{"target": {"system":
  "mysql_cdc"}}` raises the error of a system that is not a target.
- **Errors of the connector.** A `ValueError` or `ImportError` that the connector raises when the
  callback builds it propagates as the same object, with the note
  `while building the source of <path>, system 'mysql_cdc'`, as for every system.
- Building `ReadSource` builds the connector and nothing else: no Spark session, no network, no
  PyMySQL connection, no stream.

### The `mysql-cdc` extra

Following the pattern of [specs/postgres_connectors.md](postgres_connectors.md), decision 19, and
mysql_connectors decision 42; the extra is this spec's decision 42:

- `[project.optional-dependencies]` gains
  `mysql-cdc = ["PyMySQL[rsa]>=1.2.0", "mysql-replication>=1.0.16"]`. `mysql-cdc` is the system's
  name as Python normalises extra names (PEP 685); `ingestio[mysql_cdc]` installs the same.
- PyMySQL 1.2.0 or later with `rsa`, for the reasons of mysql_connectors decision 25: the
  replication connection and the control connection use the same TLS translation and
  authentication as the writer's. `mysql-replication` 1.0.16 or later, the first release that runs
  on MySQL 8.4 (B4).
- The `mysql` extra does not change: `MySQLWriter` needs no `mysql-replication`.
- Only `MySQLCDCReader` needs the extra. pyspark and Connector/J are prerequisites, never in an
  extra.
- The `dev` dependency group gains `mysql-replication>=1.0.16`. The package ships no `py.typed`
  marker and has no stubs (V5), and mypy strict checks `src/` and `tests/`, so `pyproject.toml`
  gains the override
  `[[tool.mypy.overrides]] module = ["pymysqlreplication", "pymysqlreplication.*"]` with
  `ignore_missing_imports = true`, as it has one for `google.cloud.storage`.

### Where it lives

- `src/ingestio/connector/mysql.py` (decision 27): `MySQLCDCReader`, next to `MySQLConnection`,
  `MySQLReader` and `MySQLWriter`; its sorted `__all__` becomes
  `["MySQLCDCReader", "MySQLConnection", "MySQLReader", "MySQLWriter"]`. pyspark, PyMySQL and
  `pymysqlreplication` are imported lazily, and for annotations under `TYPE_CHECKING`.
- `src/ingestio/connector/all.py`: exports `MySQLCDCReader`.
- `src/ingestio/callback/_configuration.py`: the `mysql_cdc` row of the source table, with
  `MySQLConnection` as its connection type; `binlog_position` among the callback arguments, between
  `watermark_value` and `spark`; the integer type of `server_id`.
- `src/ingestio/callback/io.py`: `ReadSource`'s `binlog_position` argument and its docstring.
- `pyproject.toml` and `uv.lock`, outside every stage's write scope: the `mysql-cdc` extra,
  `mysql-replication>=1.0.16` in the `dev` group, and the mypy override for `pymysqlreplication`
  (V5), as [the extra](#the-mysql-cdc-extra) says. The main session adds them after approval, as it
  did for the MySQL connectors.
- No change to `ingestio/__init__.py`, `ingestio/basics.py`, `connector/core.py` or the other
  connector modules.

### What becomes outdated

The documentation stage updates `docs/` and `.claude/kb/`; the README is the maintainer's.

- `docs/concepts/connectors.md`: a section for `MySQLCDCReader` (the server's settings and
  privileges, the position, the snapshot, the event format, the values, the environments, the
  limitations); the MySQL section's references to CDC as a later spec; the Extras table.
- `docs/concepts/configuration.md`: the tables of systems, keys and callback arguments (a
  `binlog_position` column), the JSON types (`server_id`, an integer), the lists of `<names>`, the
  errors, and `ReadSource`'s signature.
- `docs/concepts/callbacks.md`: `ReadSource`'s signature and arguments.
- ADR 0012, ADR 0013 and ADR 0015: see [ADRs](#adrs). `docs/index.md`: this spec, and the
  description of the connectors page.
- `.claude/kb/architecture/quick-reference.md`: modules, configuration, MySQL, public API, known
  gaps.
- `docs/backlog.md`, idea 8: its entry names this spec; the main session updates it.
- Tests of other specs that list the source systems; the test-writer of this spec updates them,
  with no change to what they check: `SOURCE_NAMES` in `tests/test_config_connectors.py`,
  `tests/test_mongodb_reader.py` and `tests/test_mysql_connectors.py`, which AT-045 of the MySQL
  spec uses.
- Validated specs whose criteria spell the list of source systems:
  [mysql_connectors](mysql_connectors.md) (AT-045, and the list under "In a configuration") and
  [mongodb_reader](mongodb_reader.md) (around line 1149). The main session amends them, as it did
  when `mysql` joined the list.
- `README.md`: the connectors table gains a `mysql_cdc` row, for the maintainer.

### Supported environments

- **pyspark** 3.5 or later, and **MySQL Connector/J** from 8.0.23 on the session's classpath, for
  the snapshot and the table's schema, as for `MySQLReader`.
- **`python-mysql-replication`** 1.0.16 or later and **PyMySQL** 1.2.0 or later, on the driver
  (decision 42).
- **Databricks classic compute** on Databricks Runtime 15.4 LTS or later, in dedicated access mode,
  as for the MySQL connectors. Databricks Connect, standard access mode and serverless compute: no
  claim.
- **MySQL** 8.4 LTS and 9.x (mysql_connectors decision 41), with `log_bin=ON`,
  `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL` (not the default, G12),
  `binlog_row_value_options` empty and `binlog_transaction_compression=OFF` (decision 33). The
  binary logs must be kept, through `binlog_expire_logs_seconds`, longer than the time between two
  runs, or the position is purged (decision 6). MariaDB: no claim.
- **Network.** The driver reaches the server's port for the control connection and the
  replication stream; the executors reach it through Connector/J.
- **Privileges**, as far as the main session and the planner know: `REPLICATION SLAVE` for the
  dump (R3), `REPLICATION CLIENT` for `SHOW BINARY LOG STATUS` and `SHOW BINARY LOGS` (R2), and
  `SELECT` on the table (decision 2).
- **`server_id`**: a number that no replica of the server and no other reader uses (R4).

### Known limitations

These are documented, not solved here.

- **The driver's memory** (decision 2). Every event of a read, and every row of the largest
  transaction, is held in the driver's memory before the DataFrame exists.
- **A table with no changes** (decision 3) does not move the position forward, so each run reads
  the binary log again from the same position, and the server can purge it (decision 6); the job
  then takes a new snapshot. An empty table on the first run gives no row, so no position: the next
  run takes a snapshot again.
- **Failover** (decision 4). A position does not survive a failover to another server, whose files
  differ; the job takes a new snapshot. The reader cannot tell that a position comes from another
  server when the file names coincide.
- **Two basenames in one index** (the main session's knowledge, not verified). The order of
  positions compares the numbers of file names across basenames (R5). If the server's index ever
  listed files of two basenames, a position in a file of the old basename could compare as at or
  after a bound in the new one, and every read would return no events, with no error, until that
  file is purged. As far as the main session knows, this needs `log_bin_index` kept while `log_bin`
  is renamed.
- **The snapshot** (decision 5). Changes made during it are read again by the next run. It is not
  frozen: every action on its DataFrame reads the table again, and a partitioned read runs several
  queries, not one consistent read.
- **No filter in the source** (G4, decision 45). The server sends the whole binary log of the range
  to the driver, every table's events included, and the library filters them; each table's reader
  reads the range again (decision 32).
- **Session settings** (G7). A session that sets `binlog_format`, `binlog_row_image`,
  `binlog_row_value_options` or `binlog_transaction_compression` for itself writes events that the
  global check does not see; the reader can miss or misread its changes.
- **The table's name as the server stores it** (G9). The library matches `table_name` exactly; a
  name in another case than the server's gives no event, with no error, while the JDBC read of the
  schema succeeds.
- **Values that differ from the JDBC read** (decision 38, G14): a `SET`'s members are joined in
  sorted order, where JDBC gives the column's order, and an empty `SET` reads as `null`, where JDBC
  gives `""` (B10); a `JSON` value is Python's text of it, which can differ from MySQL's in spacing
  and in the form of numbers, a JSON string reads without its quotes, and a JSON `null` reads as
  `null`; a `TIME` below zero or from 24 hours on gives a timestamp outside 1970-01-01; a
  `DATETIME` matches the JDBC value only under a JVM in UTC; the Spark types depend on the Spark
  version (mysql_connectors S3). An event and a snapshot of the same row can therefore hold
  different values.
- **Tables without a primary key** (decision 47). Updates are never split, and an apply by key
  cannot use their events. A change of a unique key that is not the primary key is not split
  either.
- **Changes of the table's columns** (decision 37). A column added or renamed since the position
  fails the read; the job takes a new snapshot. The values of a column dropped since are left out.
- **XA transactions and `ROLLBACK`** (decision 35, G5). An XA transaction, or a rolled-back
  transaction of a non-transactional table, that holds rows of the table fails the read.
- **Commit time** (decision 36, G3). `_ingestio_commit_time` is the header timestamp of the
  transaction's commit event, in seconds: the start of the statement that committed, which for a
  long autocommitted statement is its start, not its commit. Transactions that commit within the
  same second share it. The library exposes no commit timestamp (V1).
- **`DeprecationWarning`** (decision 43, B10). On Python 3.12 and later the library emits one for
  each `TIMESTAMP` value it decodes; under `-W error` the read fails.
- **`server_id`** (R4). An id that another replica or reader uses disconnects it.
- **The library's log lines** (decision 44, V4). `enable_logging=False` only skips the library's
  logging of the stream's parameters and of the column-name cache. Its two warnings on missing
  optional metadata and its reconnect warning remain, and go where the application's logging
  sends them.
- **Late commits.** A transaction committed after the bound is read by the next run.
- **Not verified.** R1 to R6, and G1, G3 to G5, G7 to G9 and G11 to G14, are knowledge, not
  established by a source read for this spec. The integration tests check the ones they can; the
  others stay claims to confirm.

### Notes for testing

Every criterion has at least one test that always runs, with test doubles, without a JVM, a
network or a MySQL server. The doubles live in `tests/test_mysql_cdc.py` and share one ordered event
log. No criterion needs Spark; only the integration tests do (decision 46).

- **Fake Spark session**, as in `tests/test_mysql_connectors.py`: each access to `spark.read`
  returns a new fake reader that records one *load* per `load()`, with its format and its merged
  options. A load returns a fake DataFrame whose `schema` is the *table schema* the test chooses, a
  real `pyspark.sql.types.StructType`, by default
  `StructType([StructField("id", LongType(), False), StructField("name", StringType(), True), StructField("updated_at", TimestampType(), True)])`;
  accessing `schema` is an event. `selectExpr(*exprs)` records the expressions and returns a new
  fake DataFrame. `spark.createDataFrame(data, schema)` records `list(data)` and the schema and
  returns a new fake DataFrame. Importing `pyspark.sql.types` starts no JVM.
- **Fake PyMySQL.** `pymysql.connect` replaced with `monkeypatch.setattr`; each call records its
  arguments and returns a fake connection with `cursor()`, `begin()`, `commit()`, `rollback()` and
  `close()`, each recorded. Its cursor, a context manager, records `execute(query, args=None)` and
  answers `fetchone()` and `fetchall()` for the statement executed last, from a script the test
  chooses, by default: the variables row `(1, "ROW", "FULL", "FULL", "", 0)`; `SHOW BINARY LOGS`,
  `(("binlog.000041", 1000, "No"), ("binlog.000042", 5000, "No"))`; `SHOW BINARY LOG STATUS`,
  `("binlog.000042", 5000, "", "", "")`. Any call can be made to raise.
- **Fake stream.** `pymysqlreplication.BinLogStreamReader` replaced with `monkeypatch.setattr`;
  each call records its arguments and returns a fake stream whose `fetchone()` takes the next item
  of a script of `(event, log_file, log_pos)`, sets the stream's `log_file` and `log_pos`, and
  returns the event, then `None` when the script is exhausted. `fetchone()` and `close()` are
  recorded and can be made to raise.
- **Fake events** are instances of subclasses of the library's `WriteRowsEvent`, `UpdateRowsEvent`,
  `DeleteRowsEvent`, `XidEvent` and `QueryEvent`, so that `isinstance` holds, built without the
  library's constructor, with only the attributes the reader reads ([Events](#events)). The library
  declares `rows` as a lazy property (V3), so a subclass overrides it.
- `pyspark.sql.SparkSession.active` can be replaced with `monkeypatch.setattr`. A missing package
  is simulated with `monkeypatch.setitem`, by mapping its name to `None` in `sys.modules`, or in a
  subprocess.
- **Configurations** are written with `json.dumps` to a file under `tmp_path`. A *twin* is a
  connector built directly from `ingestio.connector.all` over a second set of fakes in the same
  initial state.
- Tests use the public API and pass the fakes with `typing.cast` to satisfy mypy strict. They need
  `mysql-replication` installed, from the `dev` group, to subclass its events.

### Integration tests

On top of the criteria, real tests against a MySQL server, marked `@pytest.mark.integration` and
opt-in (decision 46), as the MySQL connectors' are (mysql_connectors decision 43). Each test
creates its own uniquely named tables and drops them. They are written blind where the stage that
writes them cannot run them. **They will not run in CI yet**: the maintainer has postponed the
MySQL service of the MySQL connectors' integration tests ([backlog idea 8](../docs/backlog.md)),
and these need it too, configured with `binlog_row_metadata=FULL` (G12), and a user with
`REPLICATION SLAVE` and `REPLICATION CLIENT`.

**Tier 1**, skipped with a reason unless `INGESTIO_MYSQL_URL` is set, as for the MySQL connectors.
No Spark: the server checks and the stream run for real, with a fake Spark session whose JDBC load
answers the table's schema, chosen by the test to match the table, and whose `createDataFrame`
records the rows.

1. From the position of `SHOW BINARY LOG STATUS`, an insert, an update and a delete in one
   transaction and in three, read as the expected events: operations, values, one position per
   transaction, equal to `SHOW BINARY LOG STATUS` right after its commit, a commit time, to the
   second, within the test's clock window (decision 36), and the sequence.
2. An update of the primary key reads as a `delete` and an `insert`.
3. Resuming from the next position of a first read returns exactly the changes committed after it,
   none twice.
4. `FLUSH BINARY LOGS` between two changes: both are read, each with its own file.
5. A position whose file number is below the oldest log fails with the purged message.
6. A column added between two changes fails the read from before it, with the column's message.
7. One row per column type: `TINYINT(1)`, `BIGINT UNSIGNED`, `DECIMAL(10,2)`, `DOUBLE`,
   `DATETIME(6)`, `TIMESTAMP(6)`, `TIME`, `YEAR`, `BIT(1)`, `BIT(12)`, `ENUM`, `SET`, `JSON`,
   `VARBINARY` and `utf8mb4` text, converted as [Values](#values) says.
8. Where the test may set it, the global `binlog_row_metadata` set to `MINIMAL` fails the read with
   its message.

**Tier 2**, skipped with a reason unless also `INGESTIO_SPARK_TESTS` is `1` and
`INGESTIO_MYSQL_JDBC_PACKAGE` is set, run in its own process, as for the MySQL connectors:

1. A snapshot holds the rows of `MySQLReader`'s full read and the position of
   `SHOW BINARY LOG STATUS` taken before it.
2. A read from the snapshot's position returns the changes made after the snapshot.
3. For each column type of tier 1's case 7, the event's value equals the value of `MySQLReader`'s
   read of the same row, except the differences listed under
   [Known limitations](#known-limitations), which the test asserts as they are.

## Acceptance criteria

Every criterion follows the maintainer's answers, recorded under [Decisions](#decisions); a
criterion that rests on one of decisions 27 to 47 names it. Unless a criterion says otherwise:

- `conn = MySQLConnection("db.internal", "shop", "ingest", "s3cret")`;
- `spark` is the fake session, and `pymysql.connect`, `pymysqlreplication.BinLogStreamReader` and
  the events are the doubles of [Notes for testing](#notes-for-testing), with their default
  answers;
- `P = ("binlog.000042", 1200)`; a *binary log read* is
  `MySQLCDCReader("shop.orders", 4001, conn, binlog_position=P, spark=spark).read()`, and a
  *snapshot read* is `MySQLCDCReader("shop.orders", 4001, conn, spark=spark).read()`;
- the **connect arguments** are
  `host="db.internal", port=3306, user="ingest", password="s3cret", database="shop", charset="utf8mb4"`,
  and the **settings** are the same without `database`;
- `V` is the statement of step 1 of [Server checks](#server-checks), exactly as written there;
- `TABLE` is the default table schema of the fake session, and `META` the five fields of
  [Reading the binary log](#reading-the-binary-log), in their order;
- `T = datetime(2026, 10, 24, 12, 0, 0, tzinfo=UTC)`, and `ts(t)` is `int(t.timestamp())`, `t`'s
  seconds since the epoch;
- `W(rows)`, `U(rows)` and `D(rows)` are `WriteRowsEvent`, `UpdateRowsEvent` and `DeleteRowsEvent`
  doubles whose `primary_key` is `"id"` and whose `rows` is `rows`; `X(n)` is an `XidEvent` double
  whose `timestamp` is `n`; `Q(text, n)` is a `QueryEvent` double whose `query` is `text` and whose
  `timestamp` is `n`, and `Q(text)` is `Q(text, ts(T))`;
- `A = datetime(2026, 10, 24, 11, 0)`, a naive datetime, and `A_UTC = A.replace(tzinfo=UTC)`;
- a script written as `(event, n)` items has `log_file` `binlog.000042` and `log_pos` `n` for each
  item; the **default transaction** is the script `(Q("BEGIN"), 1400)`,
  `(W([{"values": {"id": 1, "name": "a", "updated_at": A}}]), 1500)`, `(X(ts(T)), 1600)`;
- "exactly the options" means these keys and no others, compared case-insensitively;
- `path` is a configuration file under `tmp_path`, `<path>` is `os.fspath(path)`, and the default
  section is `{"system": "mysql_cdc", "table_name": "shop.orders", "server_id": 4001}` as `source`;
- every error of a configuration is raised by the call that builds the callback, before any
  `Process` exists.

### Import surface and the extra

- **AT-001**: Given a fresh interpreter, when `from ingestio.connector.all import MySQLCDCReader`
  runs, then it succeeds; `MySQLCDCReader` is in `ingestio.connector.all.__all__`;
  `ingestio.connector.mysql.__all__` is
  `["MySQLCDCReader", "MySQLConnection", "MySQLReader", "MySQLWriter"]` (decision 27); and
  `MySQLCDCReader` is not in `ingestio.__all__`.
- **AT-002**: Given an interpreter where `import pyspark`, `import pymysql` and
  `import pymysqlreplication` all raise `ImportError`, when `ingestio.connector.all` is imported,
  then the import succeeds, and `MySQLConnection` and a `MySQLReader` with `spark=<fake session>`
  build.
- **AT-003**: Given an interpreter where `pymysqlreplication` is mapped to `None` in `sys.modules`,
  and, separately, one where `pymysql` is, when `MySQLCDCReader("shop.orders", 4001, conn)` is
  built, then it raises `ImportError` with the message
  `MySQLCDCReader needs mysql-replication: pip install 'ingestio[mysql-cdc]'`, whose `__cause__` is
  the error of the import (decision 42). In the same interpreters,
  `MySQLCDCReader("a.b.c", 4001, conn)` raises the `ValueError` of AT-005, not the `ImportError`.
- **AT-004**: Given the metadata of the installed distribution,
  `importlib.metadata.metadata("ingestio")`, then `mysql-cdc` is among its `Provides-Extra` values;
  exactly two `Requires-Dist` carry the marker `extra == "mysql-cdc"`: one names `PyMySQL` with the
  extra `rsa` and the specifier `>=1.2.0`, the other names `mysql-replication` with the specifier
  `>=1.0.16`, the projects compared after PEP 503 normalisation; and no `Requires-Dist` with the
  marker `extra == "mysql"` names `mysql-replication` (decision 42).

### Building the reader

- **AT-005**: Given `name` in `""`, `"orders."`, `".orders"` and `"a.b.c"`, when
  `MySQLCDCReader(name, 4001, conn)` is built, then it raises
  `ValueError("table_name must be 'table' or 'database.table'")`. `"orders"` and `"shop.orders"`
  are accepted.
- **AT-006**: Given `server_id` in `0`, `-1`, `4294967296`, `True`, `1.5`, `"4001"` and `None`, when
  `MySQLCDCReader("shop.orders", server_id, conn)` is built, then it raises
  `ValueError("server_id must be an integer between 1 and 4294967295")` (decision 31). `1` and
  `4294967295` are accepted.
- **AT-007**: Given the `binlog_position` on the left, when the reader is built, then it raises
  `ValueError` with the message on the right (decision 29):

  | `binlog_position` | Message |
  |---|---|
  | `["binlog.000042", 1200]`, `("binlog.000042",)`, `("binlog.000042", 1200, 0)`, `"binlog.000042:1200"` | `binlog_position must be a (file, offset) tuple` |
  | `("binlog", 4)`, `("binlog.", 4)`, `("binlog.00004a", 4)`, `(b"binlog.000042", 4)` | `binlog_position file must be a binary log name that ends in a number, such as 'binlog.000123'` |
  | `("binlog.000042", 3)`, `("binlog.000042", True)`, `("binlog.000042", "1200")`, `("binlog.000042", 4.0)` | `binlog_position offset must be an integer of at least 4` |

  `None`, `("binlog.000042", 4)` and `("mysql-bin.log.1000000", 123456789)` are accepted.
- **AT-008**: Given `key` in `url`, `DbTable`, `QUERY`, `user`, `password`, `Driver`, `sslMode`,
  `SSLMODE`, `useSSL`, `requireSSL` and `VerifyServerCertificate`, when
  `MySQLCDCReader("shop.orders", 4001, conn, options={key: "x"})` is built, then it raises
  `ValueError` with the message `option '<key>' is set by the connector`, with the key as given. It
  accepts `{"useCursorFetch": False, "fetchsize": 500, "connectionTimeZone": "SERVER", "trustCertificateKeyStoreUrl": "file:/certs/ca.jks", "sessionInitStatement": "SET SESSION sql_mode = 'ANSI'"}`.
- **AT-009**: Given `MySQLCDCReader("a.b.c", 0, conn, binlog_position=("x", 1), options={"url": "x"})`,
  then it raises the message of `table_name`; with `"shop.orders"` instead, the message of
  `server_id`; with `4001` too, the message of the tuple's file; with `("binlog.000042", 1)`, the
  message of the offset; with `P`, the message of the option: the checks run in the order of the
  table of [Building](#building).
- **AT-010**: Given `pymysql.connect` and `pymysqlreplication.BinLogStreamReader` replaced, and
  `pyspark.sql.SparkSession.active` replaced with a function that raises, when readers with and
  without `binlog_position`, `options` and `spark` are built, then each succeeds, `pymysql.connect`,
  `BinLogStreamReader` and `active` are not called, and no attribute of `spark` is accessed.
- **AT-011**: Then `repr` and `str` of `MySQLCDCReader("shop.orders", 4001, conn)` are
  `MySQLCDCReader(table_name='shop.orders', server_id=4001, host='db.internal', database='shop')`,
  and the same given in addition `binlog_position=P` and `options={"fetchsize": 500}`.

### Server checks

- **AT-012**: When a binary log read runs, then `pymysql.connect` is called once, with exactly the
  connect arguments as keyword arguments and no positional argument; its cursor executes, in this
  order, `V`, `SHOW BINARY LOGS` and `SHOW BINARY LOG STATUS`, each with no arguments, followed by
  `fetchone()`, `fetchall()` and `fetchone()` respectively; `close()` is called once, after the
  last; `begin()`, `commit()` and `rollback()` are not called. When a snapshot read runs, the
  statements are `V` and `SHOW BINARY LOG STATUS` only. Two reads of one reader open two
  connections.
- **AT-013**: Given the variables row on the left, with or without `binlog_position`, when `read()`
  runs, then it raises `ValueError` with the message on the right; no other statement runs after
  `V`, no load and no stream are recorded, and the connection is closed (decision 33):

  | Variables row | Message |
  |---|---|
  | `(0, "ROW", "FULL", "FULL", "", 0)` | `the server's log_bin is '0'; MySQLCDCReader needs '1'` |
  | `(1, "MIXED", "FULL", "FULL", "", 0)` | `the server's binlog_format is 'MIXED'; MySQLCDCReader needs 'ROW'` |
  | `(1, "ROW", "MINIMAL", "FULL", "", 0)` | `the server's binlog_row_image is 'MINIMAL'; MySQLCDCReader needs 'FULL'` |
  | `(1, "ROW", "FULL", "MINIMAL", "", 0)` | `the server's binlog_row_metadata is 'MINIMAL'; MySQLCDCReader needs 'FULL'` |
  | `(1, "ROW", "FULL", "FULL", "PARTIAL_JSON", 0)` | `the server's binlog_row_value_options is 'PARTIAL_JSON'; MySQLCDCReader needs ''` |
  | `(1, "ROW", "FULL", "FULL", "", 1)` | `the server's binlog_transaction_compression is '1'; MySQLCDCReader needs '0'` |
  | `(0, "MIXED", "MINIMAL", "MINIMAL", "PARTIAL_JSON", 1)` | the message of `log_bin` |

  The row `("1", "row", "full", "Full", "", "0")` passes the checks.
- **AT-014**: Given the `binlog_position` on the left and the default `SHOW BINARY LOGS`, when
  `read()` runs, then it raises `ValueError` with the message on the right; `SHOW BINARY LOG STATUS`
  does not run, no load and no stream are recorded, and the connection is closed (decisions 6 and
  39):

  | `binlog_position` | Message |
  |---|---|
  | `("binlog.000041", 1001)` | `binlog_position ('binlog.000041', 1001) is past the end of 'binlog.000041', which is 1000 bytes long` |
  | `("binlog.000040", 4)` | `binlog_position ('binlog.000040', 4) has been purged from the server; its oldest binary log is 'binlog.000041'` |
  | `("binlog.000043", 4)` | `binlog_position ('binlog.000043', 4) names a binary log the server does not have; its binary logs are 'binlog.000041' to 'binlog.000042'` |
  | `("mysql-bin.000040", 4)` | `binlog_position ('mysql-bin.000040', 4) names a binary log the server does not have; its binary logs are 'binlog.000041' to 'binlog.000042'` |

  `("binlog.000041", 4)` and `("binlog.000041", 1000)` pass the check.
- **AT-015**: Given the cursor's `execute` raising `err` on `V`, then on `SHOW BINARY LOGS`, then on
  `SHOW BINARY LOG STATUS`, when a binary log read runs, then it raises `err` itself, with no note,
  and the connection is closed. Given, in addition, `close()` raising `ValueError("boom")`, then
  `err` has exactly one note,
  `the control connection could not be closed: ValueError: boom`.

### The table's schema

- **AT-016**: When a binary log read or a snapshot read runs, then exactly one load is recorded,
  and it equals the one load of the twin `MySQLReader("shop.orders", conn, spark=<twin session>).read()`:
  the same format and exactly the same options (decision 40). Given `options={"fetchsize": 500}`,
  it equals the load of the twin built with the same `options`. `schema` is accessed once on the
  load's DataFrame, and the load is recorded after the control connection's `close()`.
- **AT-017**: Given a table schema with the field on the left in addition to `TABLE`'s, when a
  binary log read and a snapshot read run, then each raises
  `ValueError("the table already has a column '<name>', which MySQLCDCReader adds to every event")`
  with `<name>` as on the left, for `_INGESTIO_Operation`, `_ingestio_binlog_file`,
  `_ingestio_binlog_offset`, `_Ingestio_Commit_Time` and `_ingestio_sequence`;
  `BinLogStreamReader`, `selectExpr` and `createDataFrame` are not called (decision 30).

### Snapshot

- **AT-018**: When a snapshot read runs, then it returns the DataFrame that `selectExpr` returned,
  called once on the load's DataFrame with exactly
  `("*", "'snapshot' AS _ingestio_operation", "'binlog.000042' AS _ingestio_binlog_file", "CAST(5000 AS BIGINT) AS _ingestio_binlog_offset", "CAST(NULL AS TIMESTAMP) AS _ingestio_commit_time", "CAST(0 AS BIGINT) AS _ingestio_sequence")`;
  `BinLogStreamReader` and `createDataFrame` are not called; and the event log shows, in this
  order, the connection, its statements, its `close()`, the load, the access to `schema` and the
  `selectExpr` (decisions 5 and 40). Given `SHOW BINARY LOG STATUS` answering a file whose name is
  the text `bin'log\x.000042`, with one quote and one backslash, the second expression is the text
  `'bin\'log\\x.000042' AS _ingestio_binlog_file`.

### Reading the binary log

- **AT-019**: When a binary log read runs, then `BinLogStreamReader` is called once, with no
  positional argument and exactly the keyword arguments `connection_settings` equal to the settings,
  `server_id=4001`, `resume_stream=True`, `log_file="binlog.000042"`, `log_pos=1200`,
  `blocking=False`, `only_events` a list holding exactly the library's `WriteRowsEvent`,
  `UpdateRowsEvent`, `DeleteRowsEvent`, `XidEvent` and `QueryEvent` classes,
  `only_schemas=["shop"]`, `only_tables=["orders"]` and `enable_logging=False` (decisions 34, 35,
  36, 41 and 44). `connection_settings` has no `database` item. Given
  `MySQLCDCReader("orders", 4001, conn, ...)`, then `only_schemas` is `["shop"]` and `only_tables`
  is `["orders"]`; given `"crm.orders"`, then `["crm"]` and `["orders"]`.
- **AT-020**: Given `SHOW BINARY LOG STATUS` answering `("binlog.000042", 1200, "", "", "")`, equal
  to `P`, when a binary log read runs, then `BinLogStreamReader` is not called, `createDataFrame` is
  called once with `[]` and `StructType([*TABLE.fields, *META])`, and `read()` returns the DataFrame
  it returned.
- **AT-021**: Given the default transaction, when a binary log read runs, then `createDataFrame` is
  called once, with the rows `[(1, "a", A_UTC, "insert", "binlog.000042", 1600, T, 1)]` and the
  schema `StructType([*TABLE.fields, *META])`, where `META` is exactly
  `StructField("_ingestio_operation", StringType(), False)`,
  `StructField("_ingestio_binlog_file", StringType(), False)`,
  `StructField("_ingestio_binlog_offset", LongType(), False)`,
  `StructField("_ingestio_commit_time", TimestampType(), True)` and
  `StructField("_ingestio_sequence", LongType(), False)`; `read()` returns the DataFrame that
  `createDataFrame` returned; `fetchone()` is called four times, and the stream's `close()` once,
  before `createDataFrame` (decisions 7, 30, 35 and 36).
- **AT-022**: Given one transaction, `(Q("BEGIN"), 1400)`,
  `(W([{"values": {"id": 1, "name": "a", "updated_at": A}}, {"values": {"id": 4, "name": "d", "updated_at": None}}]), 1500)`,
  `(U([{"before_values": {"id": 2, "name": "b", "updated_at": A}, "after_values": {"id": 2, "name": "c", "updated_at": A}}]), 1600)`,
  `(D([{"values": {"id": 3, "name": "x", "updated_at": None}}]), 1700)`, `(X(ts(T)), 1800)`, when a
  binary log read runs, then the rows are, in this order,
  `(1, "a", A_UTC, "insert", "binlog.000042", 1800, T, 1)`,
  `(4, "d", None, "insert", "binlog.000042", 1800, T, 2)`,
  `(2, "c", A_UTC, "update", "binlog.000042", 1800, T, 3)` and
  `(3, "x", None, "delete", "binlog.000042", 1800, T, 4)`: an update carries the row after it, a
  delete the row before it (decision 7).
- **AT-023**: Given `T2 = T + timedelta(seconds=1)` and `T3 = T + timedelta(seconds=2)`, and the
  default transaction followed by `(Q("BEGIN", ts(T)), 1800)`,
  `(D([{"values": {"id": 1, "name": "a", "updated_at": A}}]), 1900)`, `(X(ts(T2)), 2000)`, then
  `(Q("BEGIN", ts(T2)), 2100)`, `(W([{"values": {"id": 7, "name": "g", "updated_at": None}}]), 2200)`,
  `(Q("COMMIT", ts(T3)), 2300)`, when a binary log read runs, then the rows are
  `(1, "a", A_UTC, "insert", "binlog.000042", 1600, T, 1)`,
  `(1, "a", A_UTC, "delete", "binlog.000042", 2000, T2, 2)` and
  `(7, "g", None, "insert", "binlog.000042", 2300, T3, 3)`: each transaction carries the position
  and the header timestamp of its own commit event, not those of its `BEGIN`, and the sequence runs
  across transactions (decisions 4 and 36).
- **AT-024**: Given one transaction whose row event is
  `U([{"before_values": <before>, "after_values": <after>}])`, with the `primary_key` and the
  values on the left, when a binary log read runs, then its rows are those on the right, with the
  transaction's position and commit time and the sequences from 1 (decisions 7 and 47):

  | `primary_key` | `<before>` | `<after>` | Rows (table's values, operation) |
  |---|---|---|---|
  | `"id"` | `{"id": 2, "name": "b", "updated_at": None}` | `{"id": 5, "name": "b", "updated_at": None}` | `(2, "b", None, "delete")`, then `(5, "b", None, "insert")` |
  | `"id"` | `{"id": 2, "name": "b", "updated_at": None}` | `{"id": 2, "name": "c", "updated_at": None}` | `(2, "c", None, "update")` |
  | `("id", "name")` | `{"id": 2, "name": "b", "updated_at": None}` | `{"id": 2, "name": "c", "updated_at": None}` | `(2, "b", None, "delete")`, then `(2, "c", None, "insert")` |
  | `""` | `{"id": 2, "name": "b", "updated_at": None}` | `{"id": 5, "name": "c", "updated_at": None}` | `(5, "c", None, "update")` |

- **AT-025**: Given the default transaction with `(X(ts(T)), 1600)` replaced by
  `(Q("COMMIT", ts(T)), 1600)`, and, separately, by `(Q(" commit ", ts(T)), 1600)`, when a binary
  log read runs, then the rows are those of AT-021 (G1, decision 35). Given
  ``(Q("ALTER TABLE `shop`.`orders` ADD INDEX (name)"), 1250)`` before the default transaction, the
  rows are the same.
- **AT-026**: Given the default transaction with `(X(ts(T)), 1600)` replaced by the query event on
  the left, at offset 1600, when a binary log read runs, then it raises
  `ValueError("the binary log has rows of the table at ('binlog.000042', 1600) that no commit follows")`,
  for `Q("ROLLBACK")`, `Q("XA END X'01'")` and `Q("BEGIN")`; `createDataFrame` is not called, and
  the stream's `close()` is called once (decision 35).
- **AT-027**: Given `SHOW BINARY LOG STATUS` answering `("binlog.000042", 1600, "", "", "")` and the
  default transaction followed by `(Q("BEGIN"), 1800)`,
  `(W([{"values": {"id": 9, "name": "z", "updated_at": None}}]), 1900)`, `(X(ts(T)), 2000)`, when a
  binary log read runs, then `fetchone()` is called exactly three times, the rows are those of
  AT-021, and the stream's `close()` is called once: the read stops at the bound (decisions 3 and
  34).
- **AT-028**: Given `binlog_position=("binlog.999999", 1200)`, `SHOW BINARY LOGS` answering
  `(("binlog.999999", 9000, "No"), ("binlog.1000000", 800, "No"))`, `SHOW BINARY LOG STATUS`
  answering `("binlog.1000000", 600, "", "", "")`, and the script
  `(W([{"values": {"id": 1, "name": "a", "updated_at": None}}]), "binlog.999999", 1500)`,
  `(X(ts(T)), "binlog.999999", 1600)`,
  `(W([{"values": {"id": 2, "name": "b", "updated_at": None}}]), "binlog.1000000", 450)`,
  `(X(ts(T)), "binlog.1000000", 600)`,
  `(W([{"values": {"id": 3, "name": "c", "updated_at": None}}]), "binlog.1000000", 750)`,
  `(X(ts(T)), "binlog.1000000", 800)`, when a binary log read runs, then the rows are
  `(1, "a", None, "insert", "binlog.999999", 1600, T, 1)` and
  `(2, "b", None, "insert", "binlog.1000000", 600, T, 2)`, and `fetchone()` is called exactly four
  times: file names are ordered by their number (R5, decision 34).
- **AT-029**: Given the default transaction followed by `(Q("BEGIN"), 1800)`,
  `(W([{"values": {"id": 9, "name": "z", "updated_at": None}}]), 1900)` and the end of the script,
  when a binary log read runs, then it returns, with no error, the rows of AT-021 only
  (decision 35).
- **AT-030**: Given the default transaction with the row's values replaced by those on the left,
  when a binary log read runs, then the result is the one on the right (decision 37):

  | Values | Result |
  |---|---|
  | `{"ID": 1, "Name": "a", "updated_at": A}` | the row of AT-021 |
  | `{"id": 1, "name": "a", "updated_at": A, "my_row_id": 77}` | the row of AT-021 |
  | `{"id": 1, "name": "a"}` | `ValueError("the event of the table at ('binlog.000042', 1500) has no column 'updated_at'; the table's columns changed since it was written")`, and the stream's `close()` is called once |

- **AT-031**: Given a table schema with one field `c` of the Spark type on the left, nullable, and
  one transaction whose row event is `W([{"values": {"c": <value>}}])`, when a binary log read
  runs, then the row's first item is the one on the right (decision 38):

  | Spark type | `<value>` | First item |
  |---|---|---|
  | any of the types below | `None` | `None` |
  | `BooleanType()` | `True`; `1`; `0`; `2` | `True`; `True`; `False`; `True` |
  | `BooleanType()` | `"1"`; `"0"` | `True`; `False` |
  | `ByteType()`; `IntegerType()`; `LongType()` | `5`; `-7`; `2**63 - 1` | the same |
  | `LongType()` | `"1010"` | `10` |
  | `DecimalType(10, 2)` | `Decimal("12.50")` | `Decimal("12.50")` |
  | `DecimalType(20, 0)` | `18446744073709551615` | `Decimal("18446744073709551615")` |
  | `DoubleType()`; `FloatType()` | `1.5`; `0.25` | the same |
  | `StringType()` | `"é"` | `"é"` |
  | `StringType()` | `{"b", "a"}` | `"a,b"` |
  | `StringType()` | `{"k": [1, "é"]}`; `True`; `3`; `1.5` | `'{"k": [1, "é"]}'`; `"true"`; `"3"`; `"1.5"` |
  | `BinaryType()` | `b"\x00\x01"` | `b"\x00\x01"` |
  | `BinaryType()` | `"000100000001"` | `b"\x01\x01"` |
  | `DateType()` | `date(2026, 10, 24)`; `2026` | `date(2026, 10, 24)`; `date(2026, 1, 1)` |
  | `TimestampType()` | `A` | `A_UTC` |
  | `TimestampType()` | `datetime(2026, 10, 24, 8, 0, tzinfo=timezone(timedelta(hours=-3)))` | `datetime(2026, 10, 24, 11, 0, tzinfo=UTC)` |
  | `TimestampType()` | `timedelta(hours=8, minutes=30)` | `datetime(1970, 1, 1, 8, 30, tzinfo=UTC)` |
  | `TimestampNTZType()` | `A`; `timedelta(hours=8, minutes=30)` | `A`; `datetime(1970, 1, 1, 8, 30)` |

- **AT-032**: Given the setting of AT-031 with the Spark type and the value on the left, when a
  binary log read runs, then it raises
  `ValueError("column 'c' of the event at ('binlog.000042', 1500): cannot turn a <python type> into <spark type>")`
  with the names on the right, and the stream's `close()` is called once (decision 38):

  | Spark type | `<value>` | `<python type>`, `<spark type>` |
  |---|---|---|
  | `StringType()` | `b"x"` | `bytes`, `string` |
  | `LongType()` | `"12"` | `str`, `bigint` |
  | `LongType()` | `True` | `bool`, `bigint` |
  | `BooleanType()` | `"yes"` | `str`, `boolean` |
  | `DateType()` | `"2026-10-24"` | `str`, `date` |
  | `DateType()` | `datetime(2026, 10, 24)` | `datetime`, `date` |
  | `DecimalType(10, 2)` | `1.5` | `float`, `decimal(10,2)` |
  | `TimestampType()` | `date(2026, 10, 24)` | `date`, `timestamp` |

- **AT-033**: Given `fetchone()` raising `err` at its second call, when a binary log read runs, then
  it raises `err` itself, with no note, and the stream's `close()` is called once; given in addition
  `close()` raising `ValueError("boom")`, then `err` has exactly one note,
  `the binary log stream could not be closed: ValueError: boom`. Given `BinLogStreamReader` raising
  `err`, `createDataFrame` raising `err`, or the load raising `err`, then `read()` raises `err`
  itself. Given the default transaction and `close()` raising `err`, then `read()` raises `err`
  itself and `createDataFrame` is not called.
- **AT-034**: Given a reader built without `spark`, while `pyspark.sql.SparkSession.active` is
  replaced with a function that raises, then building succeeds. When `active` is then replaced with
  a function that returns the fake session and `read()` runs, then the load is recorded on the fake.
  Given `active` replaced with a function that raises `err` when `read()` runs, then `read()` raises
  `err` itself, and `pymysql.connect` is not called.

### In a configuration

- **AT-035**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises, when
  `ReadSource(path, connection=conn, binlog_position=P, spark=spark)` is built with the default
  `source`, then it succeeds, and `active`, `pymysql.connect` and `BinLogStreamReader` are not
  called. When `Process([Step("extract", [<that ReadSource>])]).run()` runs with the default
  transaction, then `state["records"]` is the DataFrame that `createDataFrame` returned, and the
  events equal those of the twin
  `MySQLCDCReader("shop.orders", 4001, conn, binlog_position=P, spark=<twin session>).read()`.
  Without `binlog_position`, the events equal those of the twin's snapshot read; with
  `"options": {"fetchsize": 500}` added to the section, those of the twin built with the same
  `options`.
- **AT-036**: Given the section on the left as `source`, when `ReadSource(path, connection=conn)`
  is built, then it raises `ValueError` with the message on the right (decision 31):

  | Section | Message |
  |---|---|
  | `{"system": "mysql_cdc", "server_id": 4001}` | `<path>: source.table_name is required for system 'mysql_cdc'` |
  | `{"system": "mysql_cdc", "table_name": "shop.orders"}` | `<path>: source.server_id is required for system 'mysql_cdc'` |
  | the default with `"watermark_column": "updated_at"` | `<path>: source.watermark_column is not a key of system 'mysql_cdc'` |
  | the same with `"binlog_position"`, `"host"`, `"database"`, `"user"`, `"password"`, `"port"`, `"ssl_mode"`, `"connection"` or `"fetchsize"` as the key | `<path>: source.<key> is not a key of system 'mysql_cdc'` |
  | the default with `"server_id"` set to `"4001"`, `4001.0`, `4e3`, `true`, `null` or `[4001]` | `<path>: source.server_id must be an integer` |
  | the default with `"table_name": 1` | `<path>: source.table_name must be a string` |
  | the default with `"options": {"fetchsize": [1]}` | `<path>: source.options must be an object of strings, numbers and booleans` |

- **AT-037**: Given the default `source` and the arguments on the left, when `ReadSource(path, ...)`
  is built, then it raises `ValueError` with the message on the right (decision 28):

  | Arguments | Message |
  |---|---|
  | none | `<path>: source system 'mysql_cdc' needs the argument connection` |
  | `connection=conn, client=object()` | `<path>: source system 'mysql_cdc' takes no argument client` |
  | `connection=conn, watermark_value=41` | `<path>: source system 'mysql_cdc' takes no argument watermark_value` |
  | `connection=PostgresConnection("db.internal", "shop", "ingest", "pw")` | `<path>: source system 'mysql_cdc' needs a MySQLConnection as the argument connection, not PostgresConnection` |
  | `connection=MongoDBConnection("mongodb://db.internal")` | `<path>: source system 'mysql_cdc' needs a MySQLConnection as the argument connection, not MongoDBConnection` |

  Given `binlog_position=P`, then a `source` of system `mysql` (with `connection=conn`), `postgres`
  (with a `PostgresConnection`), `disk`, `unity_catalog` and `json_api`, each with its required
  keys, raises `<path>: source system '<system>' takes no argument binlog_position`, and each builds
  with `binlog_position=None`. Given `binlog_position=P` and `client=object()` to a `mysql` source,
  the message is the one of `client`. `ReadSource(path, connection=conn, client=None,
  watermark_value=None, binlog_position=None, spark=None)` and `ReadSource(path, connection=<an
  instance of a subclass of MySQLConnection>)` build with the default `source`.
- **AT-038**: Given the default `source` changed as on the left, when
  `ReadSource(path, connection=conn, ...)` is built, then it raises the error on the right, the same
  object that the connector raises, with exactly one note,
  `while building the source of <path>, system 'mysql_cdc'`:

  | Change | Error |
  |---|---|
  | `"server_id": 0` | `ValueError("server_id must be an integer between 1 and 4294967295")` |
  | `"table_name": "a.b.c"` | `ValueError("table_name must be 'table' or 'database.table'")` |
  | `"options": {"dbtable": "x"}` | `ValueError("option 'dbtable' is set by the connector")` |
  | `binlog_position=("binlog", 4)` given to `ReadSource` | the `ValueError` of the file in AT-007 |
  | none, in an interpreter where `pymysqlreplication` is mapped to `None` in `sys.modules` | the `ImportError` of AT-003 |

- **AT-039**: Given a `source` whose `system` is `"MySQL_CDC"`, when `ReadSource(path)` is built,
  then it raises
  `ValueError("<path>: source.system 'MySQL_CDC' is not one of: bigquery, disk, gcs, json_api, mongodb, mysql, mysql_cdc, postgres, s3, unity_catalog, volume")`.
  Given the `target` `{"system": "mysql_cdc", "table_name": "shop.orders"}`, when
  `AppendOnly(path, connection=conn)` or `FullLoad(path, connection=conn)` is built, then it raises
  `ValueError("<path>: target.system 'mysql_cdc' is not one of: bigquery, disk, gcs, json_lines, mysql, postgres, s3, unity_catalog, volume")`;
  with an `scd2` section `{"key_columns": ["id"], "effective_time_column": "updated_at"}` added,
  `SCD2(path, connection=conn)` raises
  `ValueError("<path>: target.system 'mysql_cdc' is not one of: bigquery, mysql, postgres, unity_catalog")`.
- **AT-040**: Then the keyword-only parameters of `inspect.signature(ReadSource)` are, in this
  order, `into`, `connection`, `client`, `watermark_value`, `binlog_position` and `spark`, and
  `binlog_position`'s default is `None` (decision 28). `AppendOnly(path, binlog_position=P)`,
  `FullLoad(path, binlog_position=P)` and `SCD2(path, binlog_position=P)` raise Python's
  `TypeError` for an unexpected keyword argument. mypy strict accepts
  `ReadSource(path, connection=conn, binlog_position=("binlog.000042", 1200))` and a
  `MySQLCDCReader` bound to a variable annotated `Reader[DataFrame]`.

### Logging and secrets

- **AT-041**: Given `caplog` capturing every level, when readers and the callbacks of AT-035 to
  AT-039 are built, and `read()` runs as a snapshot read, as a binary log read with events, with no
  events, and failing in a server check, in the stream and in a conversion, then no log record is
  captured.
- **AT-042**: Given the readers and callbacks that AT-005 to AT-039 build, then `s3cret` appears in
  no `repr` or `str` of them, in no option `url`, and in the message or the notes of no error those
  criteria raise.

## Decisions

Taken by the maintainer on 2026-10-24, before planning. Their rationale is the technical reason the
maintainer gave; they are recorded here and not reopened.

1. **The connector only reads and returns the change events; applying them is a later
   callback.**
   - Rationale: the philosophy lists CDC replication as a callback, and
     [ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md) makes each write
     strategy one callback.
2. **Mechanism: the binary log, read on the Spark driver with `python-mysql-replication`** (PyPI
   `mysql-replication`).
   - Rationale: it captures deletes, which a watermark misses.
   - Its cost, a known limitation: it needs `binlog_format=ROW`, `binlog_row_image=FULL`,
     `binlog_row_metadata=FULL` and the replication privileges, and the events pass through the
     driver's memory before they become a DataFrame.
3. **Each event carries its binlog position; the job passes the start position as a new argument
   of `ReadSource`**, as it passes `watermark_value`, and takes the next position from the
   DataFrame, as it takes the next watermark. The reader reads up to the end of the binary log
   captured when it reads, a frozen bound, as the watermark readers freeze theirs.
   - Rationale: the same rule as the watermark, under
     [ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md): per-run
     values are callback arguments.
   - Known limitation: a table with no changes does not move the position forward, and the server
     can purge it.
4. **The position is a binlog file name and an offset**, not a GTID set. Each event carries the
   file and the offset of its transaction's commit, so the maximum the job takes is always a
   transaction boundary to resume from.
   - Rationale: it fits decision 3, since a GTID set cannot be derived with a maximum over the
     events (a set has several server UUIDs and gaps), and it does not need `gtid_mode=ON`.
   - Known limitation: it does not survive a failover to another server, whose binlog file names
     differ; the job then takes a new snapshot.
5. **First run: without a position, the reader takes a snapshot.** It captures the current binlog
   position, reads the whole table through Spark JDBC, as `MySQLReader` does, and returns each row
   as a snapshot event carrying the captured position. The next run starts from it.
   - Rationale: the job needs a starting position and a full copy, and the position must be
     captured before the copy so that no change is lost.
   - Known consequence: changes made during the snapshot are read again by the next run, which an
     apply that upserts and deletes by key tolerates.
6. **A position the server has already purged fails the read with a clear error** naming the
   position given and the oldest binlog available; the job decides to start over with a snapshot
   by passing no position.
   - Rationale: nothing changes silently (the philosophy: no failure is swallowed); an automatic
     snapshot would return something other than what the job asked for.
7. **Event format: the table's columns plus metadata columns, one row per changed row.** The
   columns hold the row after the change, and the row before it for a delete. Metadata columns
   carry at least the operation (insert, update, delete, snapshot), the binlog file, the offset and
   the commit time. The Spark schema of the table's columns comes from the table through JDBC, so
   it matches `MySQLReader`'s. An update that changes the primary key becomes a delete of the old
   key and an insert of the new one.
   - Rationale: it is the shape a `MERGE` or an apply-changes step expects, and splitting a key
     change keeps the old key from staying behind in a target applied by key.
   - The commit time is the header timestamp of the transaction's commit event, to the second
     (decision 36).

Followed from what the shipped connectors settled. Each follows the precedent named with it and is
not an open question:

8. **The job names the system in its configuration:** one more row of the closed table; the
   section's keys are the constructor arguments of the same name; credentials and per-run values
   are callback arguments
   ([ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md); backlog,
   "Shared by ideas 8 and 9").
9. **The callbacks check the type of `connection` per system**
   ([specs/mongodb_reader.md](mongodb_reader.md), decision 10; mysql_connectors decision 8): a
   `MySQLConnection` for `mysql_cdc`. `ReadSource`'s annotation already includes it.
10. **Building does nothing but validate,** and each `read()` looks up the session and connects
    anew, so that one reader serves every run (mysql_connectors decision 13).
11. **Errors propagate unchanged,** and a failure to clean up after another error becomes a note on
    it (mysql_connectors decisions 20 and 22): here, the `close()` of the control connection and of
    the stream.
12. **The connectors write no log lines,** because a connector does not know the run id
    (mysql_connectors decision 20).
13. **Secrets are never shown:** the `repr` leaves the password out, and no message holds a value
    of the table or of the connection, as for the MySQL connectors and the checks of `SCD2`.
14. **`MySQLConnection` is reused as it is:** `_connect_arguments()` for the control connection and
    the stream (mysql_connectors decision 15, built for this reuse), with its TLS translation
    (decision 29) and `charset="utf8mb4"` (decision 16); and `MySQLReader` for every JDBC read,
    with its options, read defaults and UTC sessions (decisions 30 to 35).
15. **`table_name` is `table` or `database.table`** (mysql_connectors decision 28); an unqualified
    table is in the connection's database.
16. **Statements are executed without arguments,** `cursor.execute(<statement>)`
    (mysql_connectors decision 17).
17. **Supported versions: MySQL 8.4 LTS and 9.x; no MariaDB** (mysql_connectors decision 41).
18. **The extras pattern** ([specs/postgres_connectors.md](postgres_connectors.md), decision 19;
    mysql_connectors decision 11): lazy imports; an `ImportError` naming the extra when the
    connector that needs the package is built; pyspark and the JAR are prerequisites; the `dev`
    group holds the extra's packages.
19. **Tests with doubles, and opt-in integration tests** (mysql_connectors decisions 12 and 43).
    Every criterion runs with doubles; the tests that need Spark are opt-in through
    `INGESTIO_SPARK_TESTS=1`; the MySQL service in CI is postponed by the maintainer (backlog
    idea 8).

Proposed by this spec and accepted by the maintainer on 2026-10-24, as written. Each gives its
technical reason:

20. **The DataFrame of a binary log read is built on the driver with `createDataFrame`,** from the
    events read, once the stream is closed. Reason: decision 2 puts the events in the driver's
    memory; a DataFrame built from them never reads MySQL again, so every action sees the same
    events and the read stays frozen at the bound.
21. **A position at or after the bound opens no stream.** Reason: there is nothing between them to
    read, and a dump at the end of the log would only wait for the server's EOF (B3).
22. **The stream's `log_file` and `log_pos`, right after each `fetchone()`, are the position the
    reader uses,** for the commit position and for the bound. Reason: the stream sets `log_pos`
    from each event's `packet.log_pos`, the start of the next event (B6), and a rotation sets
    `log_file` (B5); right after a commit event, that is the boundary that decision 4 asks for.
23. **The server checks, the purge check and the bound run on one PyMySQL connection, before the
    table's schema is read.** Reason: decision 5 asks for the position before the copy; a failed
    check stops the read before Spark or the stream does any work.
24. **Binary log file names are ordered by their number.** Reason: R5; a text comparison puts
    `binlog.1000000` before `binlog.999999`.
25. **The key columns of an update come from the event's `primary_key`** (B8). Reason: the table
    map of the transaction gives the key the event was written with, with no query of the reader's,
    and `binlog_row_metadata=FULL` (decision 2) is what fills it.
26. **`binlog_position` is validated by the reader, not by `ReadSource`.** Reason: the connector
    validates the values it takes, as it does `watermark_value` (Configuration, "What does not
    belong in the file").

Taken by the maintainer on 2026-10-24 when answering the 21 open questions of the first draft.
They are numbered after decision 26 so that the earlier numbers do not change: question *n* became
decision 26 + *n*. Each took the recommended option, except question 10, which took option (a)
instead of the recommended (b), because V1 refutes G2, on which (b) rested. The rationale of each
is the technical reason of the option, which the maintainer took as written; decision 36's is the
reason the maintainer gave.

27. **Names: `MySQLCDCReader`, the system `mysql_cdc`, the module `src/ingestio/connector/mysql.py`,
    and the positional arguments `(table_name, server_id, connection)`** (question 1, option (a)).
    - Rationale: PEP 8 capitalises the letters of an acronym, the reason decision 26 of the MySQL
      spec gave for `MySQL`; the system name is the class name in snake case without `Reader`
      (ADR 0015), which gives `mysql_cdc`. One module keeps the helpers the reader reuses
      (`_split_table`, `_quote`, `_check_options`, `MySQLConnection._connect_arguments`) private to
      it, where a module of its own would import private names across modules. The keys come
      before `connection`, as on `MongoDBReader`, the reader with more than one required key.
28. **The new argument of `ReadSource` is `binlog_position`, after `watermark_value`** (question 2,
    option (a)).
    - Rationale: it names the MySQL value, the "binary log position" of the manual, which a later
      CDC source would not share (a PostgreSQL LSN and a MongoDB resume token are other kinds of
      value); a system given it by mistake raises `takes no argument binlog_position`, which says
      what was wrong. It sits after `watermark_value`, the other per-run value.
29. **A position is a `tuple[str, int]`, `(file, offset)`, validated by the reader when it is
    built; the job takes the file and the offset of the event with the highest
    `_ingestio_sequence`** (question 3, option (a)).
    - Rationale: it is the smallest type that holds the two values of decision 4, typed for mypy,
      and the job builds it from two columns, of the DataFrame or of its control table, with no
      import. A value class would add a public class for two values with nothing to hide, where
      `MySQLConnection` exists to keep the password out of `repr`; a string needs a format and a
      parser; two arguments let a job give one half.
30. **Five flat metadata columns after the table's, `_ingestio_operation`,
    `_ingestio_binlog_file`, `_ingestio_binlog_offset`, `_ingestio_commit_time` and
    `_ingestio_sequence`, as the [Public API](#public-api) lists them; a table column with one of
    these names, ignoring case, fails the read** (question 4, option (a)).
    - Rationale: decision 7 asks for metadata columns, and flat columns are what a `MERGE`
      condition or an apply-changes step names directly, for the operation and the order; the
      prefix makes a clash unlikely and the check makes it explicit. The sequence gives the apply a
      total order within its batch, which several changes of one key in one batch need, and gives
      the job one column to find the last commit. Snapshot rows share 0: they have no order, and
      each key appears once. A struct column would make every condition a field access, and a
      sequence numbered per transaction would need the job and the apply to order file names by
      their number (R5), which a Spark ordering of the text does not do.
31. **`server_id` is a required key, a JSON integer from 1 to 4294967295, the constructor argument
    of the same name** (question 5, option (a)).
    - Rationale: the library requires one (B2); a duplicate disconnects the other client (R4), and
      only the job knows the ids in use on its server; a fixed id is the same in every run. A
      random id can collide with a replica's or another job's and disconnect it; the id is neither
      a secret nor a per-run value, so ADR 0015 puts it in the file; an id derived from
      `table_name` collides between readers of the same table name on one server. It brings a new
      JSON type to the configuration, an integer.
32. **One table per reader, `table_name`, as `MySQLReader`** (question 6, option (a)).
    - Rationale: decision 7 takes the schema from the table, so a DataFrame holds one table's
      columns, and decision 3 gives one position per run. Its cost: each table's reader reads the
      binary log of the range again (Known limitations).
33. **Every read checks `log_bin`, `binlog_format`, `binlog_row_image`, `binlog_row_metadata`,
    `binlog_row_value_options` and `binlog_transaction_compression`, before anything else, each
    failing with a `ValueError`** (question 7, option (b)).
    - Rationale: the library only warns when `binlog_row_metadata` is not `FULL` (B9), so without a
      check a wrong setting gives events without column names. The two other settings change what
      the binary log holds for the same change, and the library reads neither: it drops a
      compressed transaction's rows without an error, and it does not rebuild a partial JSON value
      (V2). The check turns these silent losses into errors. The snapshot run checks too, so that a
      job does not copy a table that it cannot follow. A session's own settings stay unseen (G7,
      Known limitations).
34. **The bound is `SHOW BINARY LOG STATUS` when `read()` runs; the reader stops after the first
    event at which the stream's position, file number then offset, is at or after it, or at the
    end of the stream** (question 8, option (a)).
    - Rationale: `end_log_pos` compares the offset only (B5), and the end of the stream moves with
      the writes (B3). The check after each event the reader receives is exact, because every
      transaction ends with an event the reader asks for, an `XidEvent` or a `QueryEvent`, and the
      bound is a transaction boundary (G1, to be confirmed).
35. **The table's rows are buffered until an `XidEvent` or a `QueryEvent` `COMMIT`, then stamped
    with the stream's position after it; rows not committed when the read stops are dropped;
    another `QueryEvent` while rows are buffered fails the read** (question 9, option (a)).
    - Rationale: decision 4 asks for the commit's position, and resuming inside a transaction
      breaks (B6), which rules out a position per row event. Failing keeps an XA transaction or a
      rolled-back non-transactional change (G5) from being lost in silence; dropping at the end is
      safe, because the next run starts at or before those rows.
36. **The commit time is the header timestamp of the commit event, the `XidEvent` or the
    `QueryEvent` `COMMIT`, in seconds** (question 10, option (a)).
    - Rationale: the library exposes no commit timestamp: its `GtidEvent` parses no
      `immediate_commit_timestamp`, and with `gtid_mode=OFF` no GTID event reaches the reader at
      all (V1). The header timestamp of the commit event is the closest value it gives (B8).
    - Its cost, a known limitation: the value is the start of the statement that committed, in
      seconds (G3).
    - Consequence: `GtidEvent` is not among the events the reader asks for, since it carries
      nothing the reader uses.
37. **Every column of the table's schema must be in the event, matched ignoring case, or the read
    fails; the event's columns that the schema lacks are left out** (question 11, option (b)).
    - Rationale: events then have exactly the columns of `MySQLReader`'s read (decision 7): an
      invisible column, which `SELECT *` leaves out (G8), and a column dropped since, which a
      snapshot taken now would not hold, are left out. A column the event lacks fails rather than
      being invented as `null`, and the job takes a new snapshot; the target's columns would change
      too, which no writer does (ADR 0014). It also catches events written without column names,
      whose keys are `UNKNOWN_COL<i>` (B9, V3). Requiring equal columns would fail on every table
      with an invisible column, such as a generated invisible primary key.
38. **Values are converted by the closed table of [Values](#values), by the field's Spark type and
    the value's Python type; any other pair fails the read** (question 12, option (a)).
    - Rationale: it rests only on B10 and the JDBC schema, covers the common types (`TINYINT(1)`
      as a boolean, `YEAR`, `BIT`, `TIME`, `SET`, `JSON`, `BIGINT UNSIGNED`), and fails on anything
      else instead of guessing. Its differences from the JDBC values are documented (Known
      limitations) and checked by the tier 2 tests. A table keyed by the MySQL column type would
      fix JSON strings and `null` at the cost of an API not established; rejecting tables whose
      decoded values differ from JDBC's would reject every table with a boolean column.
39. **A purged position is checked before the stream, with `SHOW BINARY LOGS`, with the three
    messages of [Server checks](#server-checks)** (question 13, option (a)).
    - Rationale: decision 6 asks the message to name the oldest binary log, which
      `SHOW BINARY LOGS` gives (R1) and the dump's error does not, as far as anyone has shown
      (B11); it also catches an offset past the end of a file, and a file of another name, before
      the dump. A purge between the check and the dump gives the server's error unchanged.
40. **The snapshot is a private `MySQLReader`'s full read, with the metadata added by `selectExpr`
    and Spark SQL literals; the same read gives the schema of a binary log read** (question 14,
    option (a)).
    - Rationale: one path for every JDBC read keeps the same options, defaults, reserved keys and
      UTC sessions (mysql_connectors decisions 30 to 35), so the snapshot and the events have the
      same table types (decision 7). `selectExpr` types the metadata with Spark's own types, the
      same as the binary log read's schema, where a JDBC query would type them through JDBC, which
      differs between Spark versions (mysql_connectors S3).
41. **The library gets `_connect_arguments()` without its `database` item** (question 15,
    option (a)).
    - Rationale: with `database`, the library's control connection opens on the job's database
      instead of `information_schema`, which the library sets (B12); the dump needs no database.
      Decision 27 of the MySQL spec left this to the CDC reader.
42. **A new extra, `mysql-cdc = ["PyMySQL[rsa]>=1.2.0", "mysql-replication>=1.0.16"]`; the `mysql`
    extra stays as it is** (question 16, option (a)).
    - Rationale: the pattern names an extra after its system and installs a package only for the
      connector that imports it, the reason decision 42 of the MySQL spec gave; `MySQLWriter` never
      imports the library. PyMySQL 1.2.0 with `rsa` for the reasons of decision 25 of the MySQL
      spec, which hold for the replication connection too. 1.0.16 is the first release shown to
      run on MySQL 8.4 (B4); the reader always passes a position, so it does not take the
      library's own `SHOW` path, but no earlier release is shown to work on 8.4 otherwise, and
      nothing shown needs 1.0.17.
43. **The library's `DeprecationWarning` is left as it is** (question 17, option (a)).
    - Rationale: the warning is about the library's own code; Python's default filters hide a
      `DeprecationWarning` raised outside `__main__`; and `catch_warnings` changes the process's
      global filters, which the Python documentation says is not thread-safe. Its cost,
      documented: under `-W error`, a `TIMESTAMP` column fails the read.
44. **The stream gets `enable_logging=False`, and the library's other log records are left to the
    application's logging** (question 18, option (a)).
    - Rationale: the connectors write no log lines (decision 12), and configuring logging is the
      application's (CONTRIBUTING, "Logging"); giving the library's loggers a `NullHandler` or
      `propagate=False` would change a third party's loggers for the whole process.
      `enable_logging=False` only skips the library's logging of the stream's parameters and of
      the column-name cache, so some of its log lines remain (V4, Known limitations).
45. **No new ADR; ADR 0012, ADR 0013 and ADR 0015 are amended, as [ADRs](#adrs) says, the ADR 0013
    amendment included** (question 19, option (a)).
    - Rationale: no decision changes the philosophy. Decision 3 applies ADR 0015's rule, per-run
      values are callback arguments, to one more value. ADR 0012's bullet of the MySQL connectors
      already names "a CDC source for MySQL, reading the binary log from a position" as a different
      connector, and its sentence on PostgreSQL describes a streaming source, which stays possible
      as another connector. The rule that changes is ADR 0013's, which an amendment records, as
      ADR 0015 amended ADR 0013 before.
    - The doc-keeper writes the text of the amendments, and the main session shows the ADR 0013
      amendment to the maintainer.
46. **Every criterion runs with test doubles, and two tiers of opt-in integration tests run against
    a real server, as [Integration tests](#integration-tests) says; they will not run in CI until
    the maintainer adds a MySQL service with `binlog_row_metadata=FULL` and a replication user**
    (question 20, option (a)).
    - Rationale: several rules rest on facts to be confirmed (R1 to R6, and the G facts that V1 to
      V6 do not settle), which only a real server checks; tier 1 checks the binary log without
      Spark, and tier 2 the values against JDBC's (decision 38). It is the structure of decision 43
      of the MySQL spec, whose service the same CI change would bring.
47. **Tables without a primary key are read, and their updates are never split** (question 21,
    option (a)).
    - Rationale: the reader returns what the binary log holds and decides no strategy (decision 1):
      an apply by key is the later callback's to refuse, and a job can keep the events as a log of
      changes. Decision 7's split needs a key, so without one there is nothing to split.

### ADRs

- **No decision changes the philosophy.** `Reader` and `Writer` are unchanged and their payload
  stays open. No new ADR is needed (decision 45).
- **ADR 0012, amended.** One more bullet in "What leans towards batch": `MySQLCDCReader.read()`
  returns one bounded read, from a position that the job keeps up to a bound frozen when it reads,
  and holds the events in the driver's memory; a streaming CDC source that tracks its own offsets in
  a checkpoint would be a different connector. The existing bullet of the MySQL connectors, which
  names the CDC reader of this spec as a different connector "reading the binary log from a
  position", stays true. **Nothing closes the door to streaming.** The doc-keeper writes it.
- **ADR 0013, amended: it changes a rule.** Its decision that "a reader ... does the filtering
  inside the source" does not hold for this reader: the replication protocol sends every event of
  the binary log (G4, to be confirmed), and the library filters the table's events on the driver
  (B7). The reason is decision 2's: the binary log is the mechanism that captures deletes. The
  maintainer accepted the amendment (decision 45); the doc-keeper writes its text, and the main
  session shows it to the maintainer.
- **ADR 0015, amended.** Its Decision lists the system names, which gain `mysql_cdc`, a source only.
  Its "What does not belong in the JSON" names `watermark_value` as a per-run value; it gains
  `binlog_position`, an argument of `ReadSource` (decision 3). The doc-keeper writes both.
- **ADR 0002 applies unchanged:** the extra follows the pattern. **ADR 0014 does not apply:** the
  reader writes nothing.

## Open questions

None open. On 2026-10-24 the maintainer answered the 21 questions of the first draft, each with the
recommended option except question 10, which took option (a) because the library exposes no commit
timestamp (V1), and accepted the proposed decisions 20 to 26 as written. The answers are decisions
27 to 47:

| Question | Answer | Decision |
|---|---|---|
| 1. Names | (a) `MySQLCDCReader`, `mysql_cdc`, in `connector/mysql.py` | 27 |
| 2. The new argument of `ReadSource` | (a) `binlog_position` | 28 |
| 3. The Python type of a position | (a) `tuple[str, int]` | 29 |
| 4. The metadata columns | (a) five flat `_ingestio_` columns | 30 |
| 5. `server_id` | (a) a required integer key | 31 |
| 6. One table per reader | (a) one table | 32 |
| 7. Checking the server's settings | (b) six variables, at every read | 33 |
| 8. How the bound is frozen | (a) `SHOW BINARY LOG STATUS`, file number then offset | 34 |
| 9. Buffering transactions until their commit | (a) other query events with rows fail | 35 |
| 10. Where the commit time comes from | (a) the commit event's header timestamp, in seconds | 36 |
| 11. A change of the table's columns | (b) every schema column in the event; extra ones left out | 37 |
| 12. Turning the decoded values into the JDBC types | (a) the closed table by Spark and Python types | 38 |
| 13. Checking for a purged position | (a) `SHOW BINARY LOGS` before the stream | 39 |
| 14. The snapshot, and reusing `MySQLReader` | (a) a private `MySQLReader` and `selectExpr` | 40 |
| 15. The connection settings given to the library | (a) without `database` | 41 |
| 16. The extra and the minimum version | (a) `mysql-cdc`, `mysql-replication>=1.0.16` | 42 |
| 17. The library's `DeprecationWarning` | (a) left as it is | 43 |
| 18. The library's logging | (a) `enable_logging=False` | 44 |
| 19. ADRs | (a) no new ADR; 0012, 0013 and 0015 amended | 45 |
| 20. Tests without a MySQL server | (a) doubles and two opt-in tiers | 46 |
| 21. Tables without a primary key | (a) read them, never split | 47 |

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| YYYY-MM-DD | planning | draft | |
| 2026-10-24 | planning | draft | Revised with the maintainer's answers: questions 1–21 became decisions 27–47 (question n is decision 26 + n), each with the recommended option except question 10, which took (a), the commit event's header timestamp in seconds, because the main session's V1 shows that the library parses no commit timestamp (G2 refuted). The proposed decisions 20–26 were accepted as written. V1–V6 cite the facts the main session read in the source of `python-mysql-replication` 1.0.17 and pyspark 4.2.0; G6, G10 and G15 confirmed. The Q10 rewrite removed `GtidEvent` from `only_events`, the doubles and AT-019, and changed AT-021 to AT-029 to match; the commit time's limitation adds that transactions committed within one second share the value. |
| 2026-10-24 | approval | approved | Pre-approved by the maintainer on 2026-10-24, on the condition that the revision only records the answers and the verified facts. The main session diffed the revision against its copy of the first draft: the same 42 criteria, no question tags left, and the criteria changed only where question 10's answer requires (the commit time from the commit event, no `GtidEvent`), besides re-wrapping. The ADR 0013 amendment (decision 45) is accepted; its text is to be shown to the maintainer at the documentation stage. |
| 2026-10-24 | tests | tested | The test-writer wrote `tests/test_mysql_cdc.py`: 78 tests and 247 cases covering AT-001 to AT-042, with a fake Spark session, fake `pymysql.connect` and `BinLogStreamReader`, and subclasses of the library's own event classes with `rows` overridden (V3), plus the two opt-in tiers (8 and 3 cases, skipped here for the reasons the spec gives; the maintainer postponed the integration tests in CI). The new tests fail on the missing `MySQLCDCReader`, the missing `mysql_cdc` system and `ReadSource`'s missing `binlog_position`; 7 cases pass by design (AT-004, and the write callbacks rejecting `mysql_cdc` and `binlog_position`). `SOURCE_NAMES` gained `mysql_cdc` in three test files, whose 7 failing cases differ only by that name. AT-004 of `mysql_connectors`'s test now counts only the `PyMySQL` requirements of the `mysql` extra, as that criterion reads, since the `mysql-cdc` extra adds one of its own. A scratch script exercised only the test doubles and the existing `MySQLReader`; no implementation was written. Checked by the main session: full suite 236 failed, 1910 passed, 151 skipped, every failure in the four files; only `tests/` changed. |
| 2026-10-24 | fact check | tested | Recorded by the main session: G4 is confirmed in `python-mysql-replication` 1.0.17, `pymysqlreplication/binlogstream.py`, `__connect_to_stream` (lines 361–365 and 426–444): the `COM_BINLOG_DUMP` request carries only the start offset, the flags, the client's `server_id` and the file name, with no filter by database or table, so the filter of B7 runs on the client. The ADR 0013 amendment (decision 45) cites it; the spec's body is unchanged. |
| 2026-10-24 | documentation | documented | The doc-keeper documented `MySQLCDCReader` in a new section of `docs/concepts/connectors.md` (the server, the position, the snapshot, the events, values, example, environments, limitations), the `mysql-cdc` extra, `configuration.md` (`binlog_position`, `server_id` as an integer, the `mysql_cdc` rows) and `callbacks.md` (`ReadSource`'s signature); amended ADRs 0012, 0013 and 0015, with no new `TODO(maintainer)`; and updated `docs/index.md` and the architecture KB. The maintainer read the ADR 0013 amendment (the filtering rule does not hold for `MySQLCDCReader`) and approved it with two changes, which the doc-keeper made: the cost is "a known limitation of the spec" instead of a cost the maintainer accepted, and the premise that the server sends every event cites the dump request of `binlogstream.py` at 1.0.17. Checked by the main session: 30 documentation examples pass and 2 fail only on the missing `MySQLCDCReader`; no added prose line runs past 100 columns. |
| 2026-10-24 | implementation | implemented | The implementer added `MySQLCDCReader` to `connector/mysql.py`, exported from `connector/all.py`, reusing `_split_table`, `_check_options`, `_connect_arguments` and `MySQLReader`, with the private helpers `_literal`, `_order`, `_is_int` and `_convert`: it checks the server's settings and the position given, then returns either a snapshot (`selectExpr` over the table's JDBC frame) or the binary log read up to the bound captured at the start, as events built with `createDataFrame`. `ReadSource` gained `binlog_position`; `_configuration.py` the `mysql_cdc` source row and the integer `server_id`. Beyond the letter of the spec: private attributes on the reader; a failing `close()` of the control connection after a successful read propagates; an empty `SHOW BINARY LOGS` passes the position check; rows are matched and converted as each row event arrives; `ReadSource`'s `connection` docstring names `mysql_cdc`; `TimestampNTZType` is checked before `TimestampType`. Checked by the main session: 2148 passed, 151 skipped, ruff, format, mypy strict and `uv lock --check` clean; only `src/` changed. |
| 2026-10-24 | validation | implemented | The acceptance-validator passed all seven checks: AT-001 to AT-042 in 236 passing cases with no opt-in criteria, the full suite 2148 passed and 151 opt-in skips, 32 documentation examples, ruff, format, mypy strict and `uv lock --check` clean; the implementer's seven points beyond the spec accepted. The 11 integration cases (8 tier 1, 3 tier 2) not run: no MySQL server and no JVM here, and the maintainer postponed them in CI. Non-blocking notes: V4 counts a `logger.info` that `enable_logging=False` skips; an empty `SHOW BINARY LOGS` is a spec gap the `log_bin` check makes unreachable; mixed binlog basenames in one index are not covered; `self._options` unused, the module docstring and `_literal`'s name and docstring; `connectors.md` (the log limitation, the file-name rule) and the ADR 0015 row of `docs/index.md`; the README. |
| 2026-10-24 | return | draft | Returned to the planner by the main session, as the maintainer prefers non-blocking notes settled before the commit. The maintainer pre-approved a revision limited to three points: correct V4 and the log limitation; record that an empty `SHOW BINARY LOGS` is unreachable after the `log_bin` check and that the position check then passes, as the code does; add mixed binlog basenames in one index as a known limitation, not verified. |
| 2026-10-24 | planning | draft | Revised on the three points of the return only: V4 corrected (the `logger.info` of line 801 is inside `__log_valid_parameters`, which `enable_logging=False` skips; three warnings remain) and the log limitation with it; an empty `SHOW BINARY LOGS` recorded as unreachable after the `log_bin` check, the position check then passing; mixed basenames in one index added as a known limitation, not verified. No criterion changed. |
| 2026-10-24 | approval | approved | Pre-approved by the maintainer on 2026-10-24 for a revision limited to those three points; the main session diffed it against its copy and found only them. |
| 2026-10-24 | tests | tested | After the return, the test-writer checked the three revised parts against `tests/test_mysql_cdc.py` and found no test they make wrong (AT-041 runs on the fake stream, so no library log record is expected; no test answers an empty `SHOW BINARY LOGS` or orders positions across basenames); nothing changed. 236 passed, 11 opt-in skips; ruff clean. |
| 2026-10-24 | documentation | documented | After the return, the doc-keeper settled the validator's notes in `docs/concepts/connectors.md`: the library's log lines as the corrected V4 says (three warnings remain), the `binlog_position` file rule with one or more characters before the dot, the new limitation "Two basenames in one index" marked not verified, and the empty `SHOW BINARY LOGS` note in the position check. It made each ADR's Status line and its row in `docs/index.md` agree, with spec amendments named in the Evidence lines: ADR 0015's row now reads "amended by 0016, 0017", ADR 0013 and ADR 0003 now say "amended by 0015". Checked by the main session: 32 documentation examples pass, and no added prose line runs past 100 columns. |
| 2026-10-24 | implementation | implemented | After the return, the implementer settled the three notes in `connector/mysql.py`: `MySQLCDCReader` validates `options` without keeping the unused `_options` attribute, the module docstring covers the binary log and `python-mysql-replication`, and the Spark SQL literal helper is `_string_literal`, with the `escapedStringLiterals` assumption in its docstring as in `unity_catalog.py`. Nothing else changed. Checked by the main session: 2148 passed, 151 skipped, ruff, format, mypy strict and `uv lock --check` clean. |
| 2026-10-24 | validation | validated | The acceptance-validator passed all seven checks again after the return: AT-001 to AT-042 in 236 passing cases, the full suite 2148 passed and 151 opt-in skips, 32 documentation examples, ruff, format, mypy strict and `uv lock --check` clean. Every non-blocking note is settled (V4 and the log limitation, the empty `SHOW BINARY LOGS`, two basenames in one index, `_options`, the module docstring, `_string_literal`, the file rule, the ADR status lines, the README) and nothing else changed. The 11 integration cases (8 tier 1, 3 tier 2) remain not run: no MySQL server and no JVM here, and the maintainer postponed them in CI. |
