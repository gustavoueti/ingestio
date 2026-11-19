# Spec: MongoDB reader, with schema hints

- **Status:** validated
- **Slug:** `mongodb_reader`. The spec is `specs/mongodb_reader.md` and its tests are
  `tests/test_mongodb_reader.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md),
  [specs/config_connectors.md](config_connectors.md),
  [specs/postgres_connectors.md](postgres_connectors.md),
  [specs/unity_catalog_connectors.md](unity_catalog_connectors.md),
  [specs/bigquery_connectors.md](bigquery_connectors.md),
  [backlog idea 9](../docs/backlog.md#9-mongodb-reader-and-writer),
  [Connectors](../docs/concepts/connectors.md),
  [Configuration](../docs/concepts/configuration.md),
  the MongoDB Spark Connector's source, [mongodb/mongo-spark](https://github.com/mongodb/mongo-spark)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

A reader for MongoDB collections, built on the MongoDB Spark Connector, so that the documents
move from MongoDB to the Spark executors. `MongoDBReader` returns a Spark DataFrame over one
collection: all of it, or, with a watermark field such as `updatedAt`, only the documents above
the job's last watermark. The filter reaches MongoDB as a `$match` stage through the connector's
filter pushdown, and `read()` freezes the slice with an upper bound that it captures first, as
`UnityCatalogReader` does. Spark infers the schema of the documents from a sample; the job fixes
the types it knows with `schema_hints`, a DDL string that the reader passes to the connector's
`schemaHints` read option. An empty or missing collection reads as a DataFrame with no columns
and no rows.

The connection string never sits in the configuration. The job reads it from its secret store at
run time and hands it to `ReadSource` in a new value, `MongoDBConnection(uri)`, through the
`connection` argument, which now takes a `PostgresConnection` or a `MongoDBConnection`. The job
names the reader in its configuration with one more system of the closed table, `mongodb`, a
source only. There is no writer, no extra and no integration test.

The maintainer answered every question of the first draft on 2026-10-24
([Decisions](#decisions) 6 to 19). The claims about the connector rest on its source code where
the main session read it ([Evidence](#evidence-from-the-connectors-source), F1 to F5); the others
are listed under [Known limitations](#known-limitations) as not verified.

## Motivation

- **A MongoDB source.** Backlog idea 9 asks for a MongoDB connector. On 2026-10-24 the maintainer
  narrowed it to a reader with schema hints, and took its engine, its credentials, its schema and
  its watermark ([Decisions](#decisions) 1 to 5). A writer, and the atomicity question it raises,
  waits for a spec of its own.
- **Documents have no schema.** The connector infers one from a sample of the collection
  (`sampleSize`, 1000 documents by default). A field that no sampled document holds is missing
  from the DataFrame, and a field whose values have different types across documents gets a type
  that the job cannot count on and that can change from one run to the next. A watermark filter
  compares a field with a value of a given type, so a wrong type gives a wrong comparison.
  `schemaHints`, in the connector's words "a partial schema of known field types to use when
  inferring the schema for the collection", lets the job fix the types of the fields it knows and
  leaves the other fields to the inference.
- **Incremental reads, filtered in the source.** Jobs ingest collections incrementally by a field
  such as `updatedAt`, and [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md)
  requires a reader to filter inside the source.
- **Laziness.** A DataFrame over a collection reads MongoDB again at every action. Without an
  upper bound, a job that counts the rows, writes them and computes its next watermark from the
  same DataFrame can see different documents each time, and record a watermark above documents it
  never wrote. The PostgreSQL, Unity Catalog and BigQuery readers capture a bound in `read()` for
  this reason, and this reader follows them (decision 5).
- **Credentials.** A MongoDB connection string carries the user and the password, and often names
  the authentication database in its path. A job reads it from a secret store at run time,
  `dbutils.secrets.get` on Databricks (decision 3); the library never reads a secret.

## Fit with the philosophy

- **Concept: connector.** `MongoDBReader` satisfies `Reader[DataFrame]`, structurally, with no
  base class and no registration ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)).
  The philosophy names MongoDB among the systems a connector integrates. `ReadSource` builds it
  from the job's configuration
  ([ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md)), through
  one more row of the closed table, which stays closed.
- **No third concept.** `MongoDBConnection` is a value, like `PostgresConnection`: it holds what
  reaching the system needs, here a connection string. It opens nothing and performs no action.
- **The reader decides no strategy, format or architecture.** Whether a run is incremental, and
  from which value, is the job's decision; the reader turns the field and the value it receives
  into a filter. It converts nothing: the columns and types of the DataFrame are those the
  connector infers, with the job's hints (decision 13). Flattening nested documents, turning `_id`
  into a string or renaming fields is formatting, which belongs to callbacks. `schema_hints` is
  the job's statement of the types its documents hold, passed to the connector as given: the
  reader chooses no type.
- **Names rendered for the engine.** The reader escapes `collection` so that it reaches the
  connector as one collection (decision 9, F5), as the other connectors quote the names they pass
  to their engine. The name itself does not change.
- **Filtering in the source.** The reader writes its filter on the DataFrame, and the connector
  pushes it to MongoDB as a `$match` stage (decision 5): `GreaterThan` and `LessThanOrEqual` are
  among the filters that the connector's documentation lists as pushed down. This is how the
  reader filters inside the source, as ADR 0013 asks. It holds when the Spark type of the
  watermark field matches the BSON type of its values, which `schema_hints` lets the job ensure
  (see [Known limitations](#known-limitations)).
- **Essential by default.** The reader catches nothing: every failure propagates out of `read()`
  and fails `ReadSource`. An empty collection is not a failure: it reads as an empty DataFrame
  (decision 19), as an empty selection of `DiskReader` does.
- **Simplicity.** Two small classes in one module, one row of the table, one widened annotation
  and one check of the connection's type. No registry, no new protocol, no new callback, no native
  client (decision 11).
- **Compose, do not impose.** The job reads its secret and builds the `MongoDBConnection`. The
  reader reads no file, environment variable or secret store. A job's own callback can still build
  a `MongoDBReader` itself, as with every shipped connector.
- **No dependencies in the core.** No new package and no extra (decision 11). pyspark and the
  MongoDB Spark Connector, a JAR, are prerequisites that the runtime provides, as pyspark and the
  PostgreSQL JDBC driver are. `ingestio` and `ingestio.connector.all` still import without
  pyspark.
- **Streaming.** Nothing here closes the door; see [ADRs](#adrs).

## Scope

In scope:

- `MongoDBConnection(uri)`: the checks of the connection string, and a `repr` that never shows
  the user or the password;
- `MongoDBReader`: full reads, and watermark reads filtered in MongoDB over a slice that `read()`
  freezes with an upper bound;
- `schema_hints`, passed to the connector's `schemaHints` read option;
- the escaping of `collection` into the connector's list syntax;
- an empty or missing collection, read as a DataFrame with no columns and no rows;
- the reader's options, and the keys it reserves;
- the `mongodb` source system: its keys, `ReadSource`'s `connection` widened to
  `MongoDBConnection`, and the check of the connection's type for every system that takes one;
- the test doubles.

Out of scope:

- writing to MongoDB, and the atomicity of such a write: a later spec (decision 1). `mongodb` is
  not a target system;
- change data capture through change streams, and streaming reads (`readStream`);
- a full `schema` key, and the JSON form of the hints (decisions 4 and 14);
- a watermark taken from the time inside the `_id` ObjectId (decision 5);
- a watermark field inside a nested document (decision 13). Hints may name nested fields; the
  reader passes them as given;
- custom aggregation pipelines, the connector's `aggregation.pipeline` (decision 16);
- reading several collections at once, which the connector's `collection` option allows (F5);
- converting, flattening, renaming or dropping fields, `_id` included (decision 13);
- checking or rewriting the path of the connection string (decision 8);
- MongoDB views, time series collections, the specifics of sharded clusters, and Atlas features
  such as Data Federation: no claim. The connector's partitioners pass through `options`, with no
  claim;
- computing, persisting or advancing the watermark, and exposing the bound that `read()` captured;
- pymongo, or any native MongoDB client (decision 11);
- integration tests against a MongoDB server (decision 18);
- retries, row counts and log lines;
- changes to `README.md`, which belong to the maintainer.

## Public API

New, in the new module `ingestio/connector/mongodb.py` (decision 6), exported from
`ingestio.connector.all` only (not from `ingestio`):

```python
from ingestio.connector.all import MongoDBConnection, MongoDBReader
```

```python
@dataclass(frozen=True, repr=False)
class MongoDBConnection:
    """Where and how to reach one MongoDB deployment. Opens nothing by itself.

    Args:
        uri: The connection string, `mongodb://...` or `mongodb+srv://...`, credentials
            included. Never shown in `repr`, `str` or an error message.
    """

    uri: str
```

```python
class MongoDBReader:
    """Reads a MongoDB collection into a Spark DataFrame, filtering inside MongoDB.

    Args:
        database: Database of the collection. The configuration key of the same name.
        collection: Source collection, one name. The configuration key of the same name.
        connection: Where the collection lives, with its credentials.
        watermark_column: Top-level field that orders changes. The configuration key of the
            same name.
        watermark_value: Last watermark already ingested. Only documents above it are read.
            Requires `watermark_column`.
        schema_hints: Types of known fields, a DDL string such as
            `"updatedAt TIMESTAMP, total DECIMAL(18,2)"`. Spark infers the other fields. The
            configuration key of the same name.
        options: Read options of the MongoDB Spark Connector passed through, such as
            `sampleSize` or `partitioner`.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        database: str,
        collection: str,
        connection: MongoDBConnection,
        *,
        watermark_column: str | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        schema_hints: str | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...

    def read(self) -> DataFrame:
        """Return a DataFrame over the documents the job needs."""
```

Changed: `ReadSource`, exported by `ingestio` and `ingestio.callback.all` as before, takes a
`MongoDBConnection` as its `connection` (decision 3):

```python
class ReadSource(Callback):
    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        into: str = "records",
        connection: PostgresConnection | MongoDBConnection | None = None,
        client: object | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...
```

- `DataFrame` and `SparkSession` are `pyspark.sql.DataFrame` and `pyspark.sql.SparkSession`.
  `Decimal` is `decimal.Decimal`; `date` is `datetime.date`, and `datetime.datetime`, its
  subclass, is accepted too. `Mapping` is `collections.abc.Mapping`.
- `database`, `collection` and `connection` are required, so they are positional, in this order,
  as `PostgresReader`'s `table_name` and `connection` are. Every other argument is keyword-only.
- `MongoDBConnection` is immutable, and `uri` is readable as an attribute, as the arguments of
  `PostgresConnection` are. The attributes of `MongoDBReader` are not documented, so they are not
  public API.
- **The configuration** gains the source system `mongodb` (decision 6): the required keys
  `database` and `collection` (decision 7); the optional keys `watermark_column`, `schema_hints`
  and `options`; the callback argument `connection`, a `MongoDBConnection`, required;
  `watermark_value` and `spark` optional; `client` not taken. `mongodb` is not a target.
- **Messages that change.** The list of source systems in
  `<path>: source.system '<name>' is not one of: <names>` gains `mongodb`. A new error, for a
  `connection` of the wrong type, is raised by `ReadSource`, `AppendOnly`, `FullLoad` and `SCD2`
  (decision 10).
- Unchanged: the signatures of `AppendOnly`, `FullLoad` and `SCD2`, whose `connection` stays a
  `PostgresConnection` (decision 3); `Reader`, `Writer`, the other connectors, everything exported
  by `ingestio`, and the extras (decision 11).

### Usage sketch

A sketch of a Databricks job, not a runnable example: `dbutils` and `last_watermark()` are the
job's. The configuration, `job.json`:

```json
{
  "source": {
    "system": "mongodb",
    "database": "shop",
    "collection": "orders",
    "watermark_column": "updatedAt",
    "schema_hints": "updatedAt TIMESTAMP, total DECIMAL(18,2)",
    "options": {"sampleSize": 5000}
  },
  "target": {"system": "unity_catalog", "table_name": "main.bronze.orders"}
}
```

The job, in the form the maintainer approved (decision 3):

```python
from ingestio import AppendOnly, Process, ReadSource, Step
from ingestio.connector.all import MongoDBConnection

uri = dbutils.secrets.get("mongo", "uri")  # the job reads its secret; the library never does
extract = ReadSource(
    "job.json", connection=MongoDBConnection(uri), watermark_value=last_watermark()
)

Process([
    Step("extract", [extract]),
    Step("load", [AppendOnly("job.json")]),
]).run()
```

## Behaviour

### Evidence from the connector's source

The main session read the source of the MongoDB Spark Connector,
[mongodb/mongo-spark](https://github.com/mongodb/mongo-spark), at the tags `r10.4.0` and
`r11.1.0`, with `gh api`, on 2026-10-24. The spec cites what it found as F1 to F5, each with the
tag and the file it was read in; a fact names a tag only where the main session gave one. "The
connector's documentation" means its v11.x pages, read the same day.

- **F1. The first release with `schemaHints` is 10.4.0.** Commit `de9753b4db` (2024-06-25),
  "Added Schema hints for use when inferring schemas. (#118)". `ReadConfig.java` at `r10.4.0`
  defines `SCHEMA_HINTS = "schemaHints"`; at `r10.3.0` the file has no `schemaHints`. The release
  notes say that 10.7 supports Spark 3.2 to 3.5, and that 11.0 adds Spark 4.0, with Scala 2.13.
  `build.gradle.kts` at `r11.1.0` compiles against Spark 4.0.1.
- **F2. Explicit `database` and `collection` beat the connection string's path.**
  `AbstractMongoConfig.createUsageOptions`, at `r10.4.0` and `r11.1.0`, first puts the database
  and the collection parsed from `connection.uri` (`addConnectionStringDatabaseAndCollection`),
  then the explicitly scoped options over them. DataFrame options reach it through
  `ReadConfig.withOptions`, which calls `withOverrides(READ_PREFIX, …)`: each key is prefixed with
  `spark.mongodb.read.`, and a key that already starts with `spark.mongodb.read.` is kept as it
  is. `MongoConfigTest.testDatabaseAndCollectionParsing` asserts the precedence twice, with the
  comment "Explicit beats implicit", at both tags. The connector's documentation says the
  opposite: "If a setting appears in both `connection.uri` and on its own line, the
  `connection.uri` setting takes precedence". The spec relies on the code.
- **F3. How the hints apply.** `InferSchema.getStructType` in `InferSchema.java`, at `r10.4.0` and
  `r11.1.0`, whose bodies are identical at the two tags (diffed), merges each hint into the struct
  of every sampled document: a hinted field that the document lacks is added, and one that it
  holds becomes `rhsPreferredMerge(inferred, hinted)`. A hint whose name holds dots, such as
  `audit.updatedAt`, builds nested structs from the top-level field. The top-level fields of the
  merged schema come out sorted by name. `ReadConfig.getSchemaHints()` in `ReadConfig.java`, with
  the same logic at `r10.4.0` and `r11.1.0`, trims the text, treats a blank text as no hints,
  parses the text as JSON when it starts with `{` and as DDL (`DataType.fromDDL`) otherwise; a
  text that does not parse raises a `ConfigException`, "Invalid schemaHints configuration: '…'".
- **F4. An empty sample.** `InferSchema.inferSchema(List<BsonDocument>, ReadConfig)` in
  `InferSchema.java`, at `r10.4.0` and `r11.1.0`, whose bodies are identical at the two tags up to
  `removePlaceholders` (diffed), reduces the structs of the sampled documents, starting from a
  placeholder. When the sample holds no document, because the collection is empty or missing, the
  hints are never applied and the inferred schema has no field.
- **F5. `collection` is a list.** `CollectionsConfig.parse`, with the same logic at `r10.4.0` and
  `r11.1.0`: the value `*` alone means every collection; otherwise `,` separates names, and `\`
  escapes `,`, `\` and `*`. Any other character after a `\`, or a `\` at the end, is a parsing
  error. More than one name, or `*`, makes `getCollectionName()` raise a `ConfigException` for a
  batch read.

### `MongoDBConnection`

**Parts of the connection string.** `MongoDBConnection` reads three parts of `uri`, as text, and
decodes nothing:

- the *scheme*: `mongodb://` or `mongodb+srv://`, which `uri` must start with, exactly, in
  lowercase;
- the *authority*: the text after the scheme, up to the first `/` or `?`, or to the end;
- the *hosts*: the text of the authority after its last `@`, or the whole authority when it holds
  no `@`, as written.

It reads nothing after the authority, path and options included: the reader sets the database and
the collection itself (decision 8).

Examples, with the user `u53r-tok` and the password `p455-tok`:

| `uri` | Hosts |
|---|---|
| `mongodb+srv://u53r-tok:p455-tok@cluster0.example.net/?retryWrites=true&w=majority` | `cluster0.example.net` |
| `mongodb://u53r-tok:p455-tok@h1:27017,h2:27017,h3:27017/shop?replicaSet=rs0` | `h1:27017,h2:27017,h3:27017` |
| `mongodb://localhost` | `localhost` |
| `mongodb://localhost:27017?directConnection=true` | `localhost:27017` |
| `mongodb://u%40ser:p%40ss@db.internal/shop.orders` | `db.internal` |
| `mongodb://u53r-tok@db.internal/admin` | `db.internal` |
| `mongodb://u53r-tok:p4@p455-tok@db.internal/?authSource=admin` | `db.internal` |

**Checks.** When the connection is built, in this order; the first that fails raises:

| Case | Error |
|---|---|
| `uri` is not a `str` | `TypeError("uri must be a str, not <type name>")` |
| `uri` does not start with `mongodb://` or `mongodb+srv://` | `ValueError("uri must start with 'mongodb://' or 'mongodb+srv://'")` |
| the text after the authority holds an `@` | `ValueError("uri has an unencoded '@' after its hosts")` |
| the hosts are empty | `ValueError("uri names no host")` |

- `<type name>` is `type(uri).__name__`.
- The third check protects the `repr`. MongoDB's connection string format requires `@`, `/`, `?`
  and `:` in the user and the password to be percent-encoded. A password with a raw `/` or `?`
  would end the authority inside the credentials, and the text after it would still hold the `@`
  that ends them: such a string is rejected rather than split into hosts that hold part of a
  password. An `@` in an option value is rejected for the same reason; it is written `%40`.
- Nothing else is checked: the path, the options, the form of each host, the credentials and the
  SRV record are the connector's and the driver's, when the reader reads. Building opens no
  connection and resolves no name.

**What it shows.** `repr(connection)` and `str(connection)` are `MongoDBConnection(hosts=<hosts>)`,
the hosts written as a Python string literal: `MongoDBConnection(hosts='cluster0.example.net')`.
No `repr`, `str` or error message of `MongoDBConnection`, of `MongoDBReader` or of the checks of
the callbacks holds any part of `uri` other than the hosts.

### The path of the connection string

The path of the connection string is accepted whatever it names, and never rewritten
(decision 8):

- **It does not decide what is read.** The reader always sets the `database` and `collection`
  options, and the connector puts them over the database and the collection it parses from the
  connection string's path (F2). So `mongodb://u53r-tok:p455-tok@db.internal/admin` with
  `database="shop"` reads `shop`, not `admin`.
- **It stays the driver's default authentication database** when the connection string has no
  `authSource` option, as MongoDB's connection string format defines it. A connection string kept
  in a secret store often names `admin` there, and keeps working.
- **The connector's documentation says otherwise.** It gives `connection.uri` precedence over a
  setting given on its own; the connector's code and its tests do the opposite (F2). The
  documentation of this reader cites F2.

### Building the reader

Building `MongoDBReader` validates its arguments and does nothing else: it imports no third-party
package, touches no Spark session, opens no connection, and does not check that the database or
the collection exists.

`schema_hints` *trimmed* is the text without its leading and trailing characters from U+0000 to
U+0020, the characters that Java's `String.trim()` removes, which is how the connector trims it
(F3) as far as the planner knows.

**Checks**, in this order; the first that fails raises `ValueError`:

| Case | Message |
|---|---|
| `database == ""` | `database must not be empty` |
| `collection == ""` | `collection must not be empty` |
| `watermark_column == ""` | `watermark_column must not be empty` |
| `watermark_value` given without `watermark_column` | `watermark_value needs watermark_column` |
| `watermark_value` is a `bool`, a `float` or a type not in [Watermark values](#watermark-values) | `unsupported watermark_value type: <type name>` |
| a `Decimal` that is NaN or infinite | `watermark_value must be finite` |
| a naive `datetime` | `watermark_value must be timezone-aware` |
| `schema_hints` trimmed is empty: `""`, or only blanks such as `"  "` or `"\t\n"` (decision 28) | `schema_hints must not be empty` |
| `schema_hints` trimmed starts with `{` (decision 14) | `schema_hints must be a DDL string, not JSON` |
| a reserved key in `options` (decision 16) | `option '<key>' is set by the connector` |

- `<type name>` is `type(value).__name__`, and `<key>` is the key as given. A datetime is naive
  when its `tzinfo` is `None` or its `utcoffset()` is `None`.
- `schema_hints` is kept as given, untrimmed, and never parsed otherwise: parsing a DDL string
  needs Spark. A hint that the connector cannot parse fails the read with the connector's
  `ConfigException` (F3).
- `MongoDBReader` does not check the type of `connection` (decision 10): a value without the
  attributes of a `MongoDBConnection` fails with Python's error.

**`repr`** shows the database, the collection as given, unescaped, and the hosts, and nothing
else, whatever else the reader is built with:

```text
MongoDBReader(database='shop', collection='orders', hosts='cluster0.example.net')
```

### Collection names

The connector reads its `collection` option as a list of names (F5). So that every collection name
reaches it as that one collection, the reader escapes `collection` before it passes the option
(decision 9): in one pass over the name, from its first character to its last, each `\`, `,` and
`*` is written with a `\` before it, and every other character is written as it is. Replacing
every `\` with `\\` first, then every `,` with `\,` and every `*` with `\*`, gives the same text;
replacing `\` last would not.

The names below are texts, not Python literals: `a\b` is three characters.

| `collection` | The `collection` option |
|---|---|
| `orders` | `orders` |
| `orders.2026` | `orders.2026` |
| `a,b` | `a\,b` |
| `*` | `\*` |
| `logs*` | `logs\*` |
| `a\b` | `a\\b` |
| `\,*` | `\\\,\*` |

- Only the option is escaped. The `repr` shows the name as given, and no error message of the
  reader shows the collection.
- `database` is passed as given.

### Reading

`read()` returns a `pyspark.sql.DataFrame`. Each call goes through these steps:

1. **Session.** The reader uses `spark` when it is given. Otherwise it calls
   `pyspark.sql.SparkSession.active()` at each `read()`, never when the reader is built. The
   reader never creates a session; when none is active, PySpark's error propagates.
2. **Source.** The reader builds one DataFrame over the collection,
   `spark.read.format("mongodb").options(**<options>).load()`, with no argument to `load()` and no
   call to `schema()`. `<options>` are, with these keys as written:
   - `connection.uri` = `connection.uri`, the connection string as given, never rewritten;
   - `database` = `database`;
   - `collection` = `collection`, escaped as [Collection names](#collection-names) says;
   - `schemaHints` = `schema_hints`, as given, only when it is given;
   - the user's `options`, as given.

   This DataFrame is the *source*.
3. **Empty schema** (decision 19). The reader reads `source.columns` once. When it is empty, the
   reader calls `where("false")` on the source and returns the DataFrame it returned, with or
   without `watermark_column`: a DataFrame with no columns and no rows. It runs no bound query.
4. **Full read** (no `watermark_column`): `read()` returns the source unchanged.
5. **Watermark read** (`watermark_column` given): the bound and the slice, below.

The reader adds no column, cast, projection or Spark setting.

**What reading the schema costs.** Classic Spark infers the schema when `load()` is called, which
reads a sample of the collection from MongoDB inside `read()`; `source.columns` then reads the
schema already known and runs no Spark job. On Spark Connect, `load()` sends nothing, and
`source.columns` sends one request to analyse the plan, which makes the server infer the schema,
sampling the collection, as far as the planner knows; each action later analyses the plan again.
Apart from that, a full read runs no Spark action, and a watermark read runs one, the bound query.

#### An empty or missing collection

- An empty collection, and a collection that does not exist, give a sample with no document, so
  the inferred schema has no field, whatever `schema_hints` says (F4). `read()` returns a
  DataFrame with no columns and no rows (step 3).
- A full read gets the same `where("false")` (decision 29): the DataFrame stays empty at every
  action. Without it, the source of a full read, which is not frozen, could show documents
  inserted after `read()` as rows with no columns on classic Spark, or with a schema inferred anew
  on Spark Connect.
- A typo in `collection` reads the same way, with no error.
- The writers of the PostgreSQL, Unity Catalog and BigQuery systems reject a DataFrame with no
  columns with `ValueError("the DataFrame has no columns")`, and a write of one by the file
  writers fails in Spark ([Connectors](../docs/concepts/connectors.md)). So a job whose first run
  reads an empty collection and then appends fails at the write. A job that wants to skip it
  checks the DataFrame in a callback of its own and raises `CancelStepException`.
- A watermark field missing from a schema that has fields, because no sampled document holds it
  and no hint names it, or because of a typo, still fails with Spark's error: on classic Spark
  inside `read()`, on Spark Connect at the bound query.

#### Watermark read

The recipe is `UnityCatalogReader`'s (decisions 5 and 11). `<col>` is the watermark field as a
Spark SQL identifier: wrapped in backticks, each backtick inside doubled. `<value>` is the literal
of `watermark_value`, built as described in [Watermark values](#watermark-values). Every
expression uses single spaces, exactly as shown.

1. **Lower predicate.** When a value is given, `<lower>` is `<col> > <value>`.
2. **Bound.** The reader takes the source, filtered with `where(<lower>)` when a value is given,
   and calls `selectExpr` on it with exactly two expressions, in this order:

   ```text
   CAST(max(<col>) AS STRING) AS upper_bound
   typeof(max(<col>)) AS upper_bound_type
   ```

   It calls `first()` on the result once and takes the two fields of the row by position: the
   bound's text, as Spark renders it, and its type, as Spark names it. The bound is the highest
   watermark of the slice, or `None` when no document qualifies.
3. **Slice.** The reader calls `where(<predicate>)` on the source, where `<predicate>` is:

   | Value given | Bound | `<predicate>` |
   |---|---|---|
   | yes | not `None` | `<col> > <value> AND <col> <= CAST(<bound> AS <type>)` |
   | no | not `None` | `<col> <= CAST(<bound> AS <type>)` |
   | either | `None` | `false` |

   `<bound>` is the bound's text as a string literal, built as for a `str` value. `<type>` is the
   type's name exactly as Spark returned it, such as `timestamp`, `bigint`, `decimal(18,2)` or
   `string`.
4. `read()` returns the DataFrame that this `where` returned, unchanged.

What this gives:

- **The filter runs in MongoDB.** Spark folds the literals and the casts of constants, so each
  comparison is between the field and a constant, which the connector pushes to MongoDB as a
  `$match` stage. The bound query sends the lower predicate to MongoDB too, and, as far as the
  planner knows, reads only the watermark field of the documents it selects, through the
  connector's column pruning; Spark computes the maximum (decision 11). An index on the watermark
  field lets MongoDB answer both `$match` stages from the index.
- **The slice is frozen.** Every action on the DataFrame reads the same range of the watermark.
  Documents written after `read()` with a watermark above the bound are left for the next run, so
  a watermark that the job computes from the DataFrame never goes past what the DataFrame holds.
- **The bound round-trips** for integer, decimal, string and timestamp fields: Spark renders the
  highest value as text and casts that text back to the field's own Spark type, so the bound never
  passes through Python types. For a `TIMESTAMP` field the text is the session time zone's wall
  clock; see [Known limitations](#known-limitations).
- **Documents whose watermark is `null` or missing are never read** when `watermark_column` is
  given, first run included: Spark reads a missing field as `null`, and `null` satisfies neither
  comparison.
- **An empty slice** (no document above the value, or none with the field) is the source filtered
  with `false`: a DataFrame with the source's columns and no rows.

#### Watermark values

As `UnityCatalogReader`'s (decision 12). `watermark_value` is "given" when it is not `None`; `0`
and `""` are values. A value becomes a Spark SQL literal:

| Type | Literal |
|---|---|
| `int`, but not `bool` | `str(value)`, unquoted: `41`, `-7` |
| `decimal.Decimal`, finite | `format(value, "f")` followed by `BD`: `12.50BD` |
| `str` | a string literal of the string |
| `datetime.datetime`, timezone-aware | a string literal of `value.astimezone(UTC).isoformat(sep=" ")` |
| `datetime.date` | a string literal of `value.isoformat()` |

A **string literal** is the text wrapped in single quotes, with each backslash and each single
quote inside preceded by a backslash: `O'Brien` becomes `'O\'Brien'`, and `C:\temp` becomes
`'C:\\temp'`. This assumes `spark.sql.parser.escapedStringLiterals` is `false`, Spark's default.

- **Which value for which field.** The value has the field's Spark type, or one that Spark
  coerces to it: an aware `datetime` for a field of BSON type Date, read as `TIMESTAMP`; an `int`
  for Int32 and Int64; a `Decimal` for Decimal128; a `str` for String. Spark casts a string
  literal compared with a `TIMESTAMP` field to a timestamp, so an aware datetime, converted to
  UTC, is the same instant whatever offset it carried. A `date` compared with a `TIMESTAMP` field
  is midnight of that date in the session time zone.
- **The reader writes Spark literals, not MongoDB queries.** The connector turns each pushed
  comparison into BSON values.

#### `_id`, nested documents and the schema

- **Hints.** `schema_hints` reaches the connector as `schemaHints`, unchanged. Its documented form
  is DDL, `<name> <type>` pairs separated by commas: `updatedAt TIMESTAMP, total DECIMAL(18,2)`
  (decisions 4 and 14). The connector merges each hint into every sampled document: a hinted
  field that a document lacks is added, and one that it holds is merged with the hint, the hint
  preferred (F3). A hint name may be a dotted path into nested documents, such as
  `audit.updatedAt TIMESTAMP`, which the connector turns into nested structs (F3); the reader
  passes it as given and claims nothing more. With an empty sample the hints are not applied (F4).
- **Column order.** The top-level fields of the merged schema come out sorted by name (F3): the
  DataFrame's columns are not in the order of the documents' fields.
- **Everything else is the connector's** (decision 13). The reader converts nothing. How the
  connector infers a field whose types differ across documents, nested documents, arrays and
  `_id` (its documentation shows an ObjectId `_id` inferred as `struct<oid: string>`) gives the
  DataFrame's schema, as the connector builds it. A job that needs other types hints them; one
  that needs another shape formats the DataFrame in a callback of its own.
- **Sampling** is the connector's: `sampleSize` and the `sql.inferSchema.*` options pass through
  `options`.
- **The watermark field** is one top-level field, quoted as one name (decision 13). A field of a
  nested document, or a name that holds a `.`, is not supported as the watermark field.

### Options

- `options` are read options of the MongoDB Spark Connector, passed through as given, such as
  `sampleSize`, `partitioner`, `partitioner.options.*`, `aggregation.allowDiskUse`, `mode`,
  `columnNameOfCorruptRecord`, `outputExtendedJson`, `sql.inferSchema.mapTypes.*` and `comment`.
  The connector prefixes each of them, and each of the reader's, with `spark.mongodb.read.` (F2).
  The reader sets no option besides its own keys, which are reserved, so no user option can
  replace them.
- Values reach Spark as given. Spark turns them into strings, and booleans into `true` and
  `false`.
- **Reserved keys** (decision 16). Giving one raises `ValueError("option '<key>' is set by the
  connector")` when the reader is built, with `<key>` as given. A key is reserved when its
  lowercase form is the lowercase form of one of the names below, alone or preceded by
  `spark.mongodb.read.` or by `spark.mongodb.`:

  | Reserved key | Why |
  |---|---|
  | `connection.uri` | It comes from `connection`. |
  | `database`, `collection` | The reader names the collection. |
  | `schemaHints` | It comes from `schema_hints`. |
  | `aggregation.pipeline` | A pipeline changes what is read and cannot safely be bounded by a watermark ([ADR 0013](../docs/decisions/0013-connector-configuration-convention.md)), as `query` is reserved on the PostgreSQL and BigQuery readers. |

  A key that already starts with `spark.mongodb.read.` reaches the connector as it is (F2), so
  `spark.mongodb.read.database` would compete with the reader's own `database`, which the
  connector prefixes to the same key.

### Errors of a read

Nothing is caught. Errors of PySpark, Spark, the connector and MongoDB propagate out of `read()`
unchanged, as the same object. When they surface depends on the session: classic Spark infers the
schema when `read()` loads the source, so, as far as the planner knows, a server that cannot be
reached, a failed authentication or a hint that cannot be parsed (the connector's
`ConfigException`, F3) fails `read()`; on Spark Connect they fail when the plan is first analysed,
at `source.columns` inside `read()`. The reader adds nothing to these errors and redacts nothing
in them. It writes no log lines.

### In a configuration

The `mongodb` system (decision 6) is one more row of the closed table, a source only:

| `system` | `source` builds | `target` builds | Extra the connector may need |
|---|---|---|---|
| `mongodb` | `MongoDBReader` | not a target | none (decision 11) |

| `system` | Section | Required keys | Optional keys |
|---|---|---|---|
| `mongodb` | source | `database`, `collection` | `watermark_column`, `schema_hints`, `options` |

| `system` | Section | `connection` | `client` | `watermark_value` | `spark` |
|---|---|---|---|---|---|
| `mongodb` | source | **required**: a `MongoDBConnection` | no | optional | optional |

- **Keys** (decision 7). Each key is the constructor argument of the same name, passed as it is.
  `database`, `collection` and `schema_hints` are JSON strings, checked as the other string keys
  are (`<path>: source.<key> must be a string`); `watermark_column` and `options` keep their
  types. The connection string, the user, the password and the hosts are never keys: `uri`, like
  any key the system does not take, raises `<path>: source.uri is not a key of system 'mongodb'`.
- **Lists of systems.** `<names>` for a source becomes
  `bigquery, disk, gcs, json_api, mongodb, postgres, s3, unity_catalog, volume`. For a target it is
  unchanged, so `{"target": {"system": "mongodb", ...}}` raises
  `<path>: target.system 'mongodb' is not one of: bigquery, disk, gcs, json_lines, postgres, s3, unity_catalog, volume`.
- **`connection`.** `ReadSource` hands the very object to `MongoDBReader`. When it is missing, the
  build raises `<path>: source system 'mongodb' needs the argument connection`.
- **The type of `connection`** (decision 10). For every system that takes `connection`, the
  callback checks that it is an instance of that system's connection class, after the other checks
  of the callback arguments and before it builds the connector. Otherwise it raises
  `ValueError("<path>: <section> system '<system>' needs a <class> as the argument connection, not <type name>")`,
  where `<class>` is `MongoDBConnection` for `mongodb` and `PostgresConnection` for `postgres`, and
  `<type name>` is `type(connection).__name__`. So a `MongoDBConnection` given to a `postgres`
  source raises
  `<path>: source system 'postgres' needs a PostgresConnection as the argument connection, not MongoDBConnection`,
  and the same holds for `AppendOnly`, `FullLoad` and `SCD2` with a `postgres` target. A subclass
  of the class is accepted.
- **Errors of the reader.** A `ValueError` that `MongoDBReader` raises when the callback builds it,
  such as a reserved option, propagates as the same object, with the note
  `while building the source of <path>, system 'mongodb'`, as for every system.
- Building `ReadSource` builds the reader and nothing else: it touches no Spark session and no
  network.

### Where it lives

- `src/ingestio/connector/mongodb.py` (decision 6): `MongoDBConnection` and `MongoDBReader`, with
  a module docstring and a sorted `__all__`. pyspark is imported lazily, and for annotations under
  `TYPE_CHECKING`.
- `src/ingestio/connector/all.py`: exports both.
- `src/ingestio/callback/_configuration.py`: the `mongodb` row, the string keys `database`,
  `collection` and `schema_hints`, and the check of the connection's type (decision 10).
- `src/ingestio/callback/io.py`: the annotation and the docstring of `ReadSource`'s `connection`.
- No change to `pyproject.toml` or `uv.lock` (decisions 11 and 18), `ingestio/__init__.py`,
  `ingestio/basics.py`, `connector/core.py` or the other connector modules.

### What becomes outdated

The documentation stage updates `docs/` and `.claude/kb/`; the README is the maintainer's.

- `docs/concepts/connectors.md`: a MongoDB section, which cites F2 for the path of the connection
  string, and the Extras section, which says that MongoDB needs none.
- `docs/concepts/configuration.md`: the tables of systems, keys, JSON types and callback
  arguments; the description of `connection` ("a `PostgresConnection`, required by the `postgres`
  system"); the lists of `<names>`; the new error of the connection's type (decision 10).
- `docs/concepts/callbacks.md`: `ReadSource`'s `connection`.
- ADR 0012, ADR 0013 and ADR 0015: see [ADRs](#adrs). `docs/index.md`: this spec, and the
  description of the connectors page.
- `.claude/kb/architecture/quick-reference.md`: the modules, the configuration lines and the
  public API.
- `docs/backlog.md`, idea 9: its entry links to this spec; the writer and change streams stay in
  the backlog as open ideas. The main session edits it.
- `tests/test_config_connectors.py`: its constant `SOURCE_NAMES` holds the old list of source
  systems. The test-writer of this spec updates it.
- `README.md`: the connectors table gains a `mongodb` row, for the maintainer.

### Supported environments

- **pyspark** 3.5 or later, a prerequisite that the runtime provides.
- **The MongoDB Spark Connector**, a prerequisite JAR,
  `org.mongodb.spark:mongo-spark-connector_<Scala version>:<version>`, on the session's classpath
  (decision 15):
  - for Spark 3.5, Databricks Runtime 15.4 LTS included, a 10.x release from 10.4.0, the first
    that reads `schemaHints` (F1), and which applies the hints as F3 and F4 describe;
  - for Spark 4, the 11.x line (F1). `r11.1.0` compiles against Spark 4.0.1; support for Spark
    4.2, the pyspark of `uv.lock`, is not established.
  - The reader checks no version at run time.
- **Databricks classic compute** on Databricks Runtime 15.4 LTS or later, in dedicated access
  mode, with the connector installed as a cluster library. Standard access mode, which restricts
  JAR libraries, and serverless compute, which cannot install one as far as the planner knows: no
  claim. Only the DataFrame API is used.
- **Databricks Connect** against such a cluster: not tested, no claim.
- **Open-source Spark** 3.5 or later, on a Java version that release supports (17 or 21 for
  Spark 4), with the connector on the classpath, for example through `spark.jars.packages`.
- **MongoDB**: the server versions that the connector supports. The reader adds no requirement of
  its own.
- **Network.** The driver reaches MongoDB to sample the collection and plan the partitions, and
  every executor reaches it to read its partitions.
- **Privileges.** The connector runs aggregations, `$sample` for the inference, and the
  statistics of its default partitioner. The built-in `read` role on the database covers them, as
  far as the planner knows.

### Known limitations

These are documented, not solved here.

**Not verified.** This spec has no integration tests (decision 18), and the planner could not run
the connector. What F1 to F5 establish is cited where it is used. These claims rest on the
connector's documentation and the planner's knowledge only:

- that the connector pushes `<col> > <literal>` and `<col> <= <literal>` on `timestamp`, `bigint`,
  `decimal` and `string` fields to MongoDB as one `$match`, and that the bound query reads only
  the watermark field;
- whether Spark applies the pushed comparisons again to the rows it receives, which decides what a
  type mismatch does (below);
- how the connector converts a watermark value with microseconds into a BSON Date;
- when the errors of inference surface, on classic Spark and on Spark Connect;
- what Spark's redaction covers, and whether the connector's errors ever hold a part of the
  connection string (below);
- the privileges the connector needs, and the environments other than Databricks classic compute
  in dedicated access mode and open-source Spark.

**Reading.**

- **Late writes.** A document written after a read with a watermark at or below the bound that
  read captured is never read by a later run. Documents that share the exact value of the previous
  watermark but are written later are missed the same way.
- **The slice is frozen by watermark, not by snapshot.** Each action reads MongoDB again, each
  partition with its own cursor. A document of the slice that is deleted, or updated without
  moving its watermark, after `read()` shows its new state in later actions. A full read is not
  frozen at all.
- **Type mismatch.** MongoDB compares a value only with values of the same BSON type class: a
  `$match` on a String value does not match a Date. When the Spark type of the watermark field
  does not match the BSON type of its values, for instance a field inferred as a string because
  some documents hold strings, the pushed filter can leave out documents that Spark would have
  selected. The job hints the watermark field with the type its documents hold, and keeps one BSON
  type in that field.
- **Inference by sampling.** A field that no sampled document holds is absent from the DataFrame
  unless it is hinted (F3), and the type of a field that is not hinted can change from one run to
  the next.
- **An empty or missing collection** reads as a DataFrame with no columns and no rows (F4), a typo
  in `collection` included, and a job that appends it fails at the write (see
  [An empty or missing collection](#an-empty-or-missing-collection)).
- **Milliseconds.** A BSON Date holds milliseconds. A watermark of millisecond precision avoids the
  question of how the connector converts a value with microseconds.
- **`TIMESTAMP` bounds and the session time zone**, as for `UnityCatalogReader`: Spark renders the
  bound in the session time zone and casts it back in the same zone, so the session time zone must
  not change between `read()` and the actions on the DataFrame.
- **The bound query** brings into Spark the watermark field of every document above the lower
  bound, once per `read()`: MongoDB does not compute the maximum (decision 11).
- **Reading the schema** costs one more analysis of the plan on Spark Connect, which samples the
  collection again, at every `read()` (decision 29).
- **`mode`.** With `PERMISSIVE` or `DROPMALFORMED` in `options`, documents that do not fit the
  schema are kept aside or dropped instead of failing the read. Those at or below the bound are
  never read again (decision 16).
- **`_id`.** The time inside an ObjectId is not a watermark (decision 5), and a field that holds
  ObjectIds cannot be compared with any accepted value.

**Credentials** (decision 17).

- The connection string, credentials included, reaches Spark as the value of the `connection.uri`
  option. Spark's default redaction of option values in its plans covers keys that match `url`,
  as far as the planner knows, which `connection.uri` does not. Whether `explain()`, the Spark UI
  or the event logs show it, and whether the connector redacts it, is not established. A job that
  needs it hidden adds `uri` to the session's `spark.sql.redaction.options.regex`, for example
  `spark.conf.set("spark.sql.redaction.options.regex", "(?i)url|uri")`.
- Errors of the connector and of the driver propagate unchanged; whether they ever hold a part of
  the connection string is not established.

### Notes for testing

Every criterion has at least one test, and every test runs with test doubles, without a JVM, a
network or a MongoDB server. There are no integration tests (decision 18). The doubles live in
`tests/test_mongodb_reader.py` and share one ordered event log.

- **Fake Spark session.** Each access to `spark.read` returns a new fake reader, as PySpark does.
  It supports `format(source)`, `option(key, value)`, `options(**options)`, `schema(schema)` and
  `load(*args, **kwargs)`. Each `load()` records a *load*, with the format, the merged options and
  the arguments of `load()`; `schema()` records a *schema*. A later option replaces an earlier one
  with the same name, compared case-insensitively as Spark does; the original case of every key is
  kept. Values are compared as Spark sees them: `str(value)`, and booleans as `true` and `false`.
  `load()` returns a new fake DataFrame, the *source*, whose `columns` the test chooses: by
  default `["_id", "total", "updatedAt"]`. The fake records every attribute accessed on the
  session.
- **Fake DataFrame.** It records every attribute accessed on it, `columns` included, and every
  event records which DataFrame it was called on. `where(condition)` and `filter(condition)`
  record a *where* with the condition and return a new fake DataFrame; `selectExpr(*expr)` records
  a *select* and returns a new fake DataFrame, on which `first()` records a *row access* and
  answers the single row the test chose, such as `("2026-10-21 09:30:00.5", "timestamp")` or
  `(None, "timestamp")`; `head()`, `collect()` and `take(n)` record a row access too. These are
  the doubles of `tests/test_unity_catalog_connectors.py`, with `load()` in place of `table()` and
  a configurable `columns`.
- **Configurations** are written with `json.dumps` to a file under `tmp_path`, as in
  `tests/test_config_connectors.py`. A *twin* is a `MongoDBReader` built directly from
  `ingestio.connector.all` over a second fake session in the same initial state; a criterion that
  maps the section compares the events of the callback's reader with those of its twin, in order.
- `pyspark.sql.SparkSession.active` can be replaced with `monkeypatch.setattr`. A missing pyspark
  is simulated in a subprocess, by mapping `pyspark` to `None` in `sys.modules`.
- The connection strings of the criteria carry the user `u53r-tok` and the password `p455-tok`,
  tokens that appear nowhere else, so that a test can check that a text holds neither.
- Tests use the public API. They pass the fakes as `spark=` and as DataFrames, with `typing.cast`
  to satisfy mypy strict, which checks `tests/` too.
- **Other test files.** The check of the connection's type (decision 10) leaves the existing tests
  as they are: they give a `PostgresConnection` to the `postgres` system. `SOURCE_NAMES` in
  `tests/test_config_connectors.py` changes (see [What becomes outdated](#what-becomes-outdated)).

## Acceptance criteria

Every criterion follows the maintainer's answers, recorded under [Decisions](#decisions). Unless a
criterion says otherwise:

- `URI` is `mongodb+srv://u53r-tok:p455-tok@cluster0.example.net/?retryWrites=true&w=majority`,
  and `conn` is `MongoDBConnection(URI)`;
- `spark` is the fake session, and DataFrames are the doubles of
  [Notes for testing](#notes-for-testing); the source's `columns` is
  `["_id", "total", "updatedAt"]`;
- the reader is `MongoDBReader("shop", "orders", conn, spark=spark)`, with whatever the criterion
  adds;
- *the base options* are `connection.uri` = `URI`, `database` = `shop` and `collection` = `orders`;
- in a watermark read, `watermark_column="updatedAt"`, and the bound query answers
  `("2026-10-21 09:30:00.5", "timestamp")`;
- "exactly the options" means these keys and no others, compared case-insensitively unless the
  criterion says the case matters;
- names and option values written in code font are texts, not Python literals: `a\b` is three
  characters;
- `path` is a configuration file under `tmp_path`, `<path>` is `os.fspath(path)`, and its `source`
  section is `{"system": "mongodb", "database": "shop", "collection": "orders"}`;
- every error of a configuration is raised by the call that builds the callback, before any
  `Process` exists.

### Import surface

- **AT-001**: Given a fresh interpreter, when
  `from ingestio.connector.all import MongoDBConnection, MongoDBReader` runs, then it succeeds.
  Both names are in `ingestio.connector.all.__all__` and in `ingestio.connector.mongodb.__all__`,
  and neither is in `ingestio.__all__`. `ingestio.ReadSource` is
  `ingestio.callback.all.ReadSource`.
- **AT-002**: Given an interpreter where `import pyspark` raises `ImportError`, when
  `ingestio.connector.all` is imported, then the import succeeds; `MongoDBConnection(URI)` and
  `MongoDBReader("shop", "orders", conn, spark=<fake session>)` can be built, and `read()` returns
  the DataFrame of the fake's load.

### `MongoDBConnection`

- **AT-003**: Given each `uri` of the examples in [`MongoDBConnection`](#mongodbconnection), when
  `MongoDBConnection(uri)` is built, then it succeeds, its `uri` is the very string given, and both
  `repr` and `str` of it are `MongoDBConnection(hosts='<hosts>')`, with the hosts of the table.
  Assigning to its `uri` raises `dataclasses.FrozenInstanceError`.
- **AT-004**: Given the `uri` on the left, when `MongoDBConnection(uri)` is built, then it raises
  the error on the right:

  | `uri` | Error |
  |---|---|
  | `b"mongodb://db.internal"` | `TypeError("uri must be a str, not bytes")` |
  | `None` | `TypeError("uri must be a str, not NoneType")` |
  | `""` | `ValueError("uri must start with 'mongodb://' or 'mongodb+srv://'")` |
  | `"MONGODB://db.internal"` | the same |
  | `" mongodb://db.internal"` | the same |
  | `"mongodb:/db.internal"` | the same |
  | `"postgresql://u53r-tok:p455-tok@db.internal/shop"` | the same |
  | `"mongodb://u53r-tok:p4/55-tok@db.internal/"` | `ValueError("uri has an unencoded '@' after its hosts")` |
  | `"mongodb://u53r-tok:p4?55-tok@db.internal/"` | the same |
  | `"mongodb://db.internal/?appName=svc@corp"` | the same |
  | `"mongodb://"` | `ValueError("uri names no host")` |
  | `"mongodb:///shop"` | the same |
  | `"mongodb://u53r-tok:p455-tok@/shop"` | the same |
  | `"mongodb+srv://?tls=true"` | the same |

### Building the reader

- **AT-005**: Given the arguments on the left, when `MongoDBReader` is built, then it raises
  `ValueError` with the message on the right:

  | Arguments | Message |
  |---|---|
  | `("", "orders", conn)` | `database must not be empty` |
  | `("shop", "", conn)` | `collection must not be empty` |
  | `("shop", "orders", conn, watermark_column="")` | `watermark_column must not be empty` |
  | `("shop", "orders", conn, watermark_value=41)` | `watermark_value needs watermark_column` |

- **AT-006**: Given the `schema_hints` on the left, Python literals here, when
  `MongoDBReader("shop", "orders", conn, schema_hints=...)` is built, then it raises `ValueError`
  with the message on the right:

  | `schema_hints` | Message |
  |---|---|
  | `""` | `schema_hints must not be empty` |
  | `"   "` | the same |
  | `"\t\n"` | the same |
  | `'{"type": "struct", "fields": []}'` | `schema_hints must be a DDL string, not JSON` |
  | `'  {"type": "struct"}'` | the same |
  | `'\n{"type": "struct"}'` | the same |

  `"updatedAt TIMESTAMP, total DECIMAL(18,2)"`, `"audit.updatedAt TIMESTAMP"`,
  `"STRUCT<updatedAt: TIMESTAMP>"`, `"  updatedAt TIMESTAMP  "` and `"not ( a ddl"` are accepted.
- **AT-007**: Given `watermark_column="updatedAt"` and the `watermark_value` on the left, when the
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
- **AT-008**: Given `key` in `connection.uri`, `Connection.URI`, `database`, `DATABASE`,
  `collection`, `schemaHints`, `schemahints`, `aggregation.pipeline`,
  `spark.mongodb.read.connection.uri`, `spark.mongodb.read.database`, `spark.mongodb.collection`
  and `Spark.MongoDB.Read.SchemaHints`, when
  `MongoDBReader("shop", "orders", conn, options={key: "x"})` is built, then it raises
  `ValueError` with the message `option '<key>' is set by the connector`, with the key as given.
  The reader accepts `{"sampleSize": 5000}`,
  `{"partitioner": "com.mongodb.spark.sql.connector.read.partitioner.PaginateBySizePartitioner", "partitioner.options.partition.size": 64}`
  and `{"aggregation.allowDiskUse": True, "mode": "FAILFAST", "comment": "ingestio", "outputExtendedJson": False}`.
- **AT-009**: Given `MongoDBConnection(uri)` for each `uri` in
  `mongodb://u53r-tok:p455-tok@db.internal/admin`, `mongodb://db.internal/shop.events`,
  `mongodb://db.internal/other.orders?authSource=admin` and `mongodb://db.internal/shop`, when
  `MongoDBReader("shop", "orders", <that connection>, spark=spark)` is built and reads, then
  building succeeds, and the load carries exactly the options `connection.uri` = `uri`, unchanged,
  `database` = `shop` and `collection` = `orders`.
- **AT-010**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises, when
  readers are built with and without `spark=spark`, with and without a watermark, and with
  `schema_hints` and `options`, then each succeeds, `active` is not called, and no attribute of
  `spark` is accessed.
- **AT-011**: Then `repr(reader)` and `str(reader)` are
  `MongoDBReader(database='shop', collection='orders', hosts='cluster0.example.net')`, also when
  the reader is built with `watermark_column="updatedAt"`, `watermark_value=41`,
  `schema_hints="updatedAt TIMESTAMP"` and `options={"sampleSize": 5000}`. A reader built with the
  collection `a,b*` has the `repr`
  `MongoDBReader(database='shop', collection='a,b*', hosts='cluster0.example.net')`.

### Full read

- **AT-012**: When `reader.read()` runs, then exactly one load is recorded, with the format
  `mongodb`, exactly the base options, with the keys `connection.uri`, `database` and `collection`
  in this case, and no argument to `load()`. No schema is recorded. `read()` returns the very
  DataFrame that `load()` returned; the only attribute accessed on it is `columns`, once. The only
  attribute accessed on `spark` is `read`.
- **AT-013**: Given `schema_hints="  updatedAt TIMESTAMP, total DECIMAL(18,2)"` and
  `options={"sampleSize": 5000, "Partitioner": "x"}`, when the reader reads, with or without a
  watermark, then exactly one load is recorded, and it carries exactly the base options,
  `schemaHints` = `  updatedAt TIMESTAMP, total DECIMAL(18,2)`, unchanged, `sampleSize` = `5000`
  and `Partitioner` = `x`, with the keys `schemaHints` and `Partitioner` in this case. A reader
  built without `schema_hints` records no `schemaHints` option.
- **AT-014**: Given a reader built without `spark`, while `pyspark.sql.SparkSession.active` is
  replaced with a function that raises, then building succeeds. When `active` is then replaced
  with a function that returns the fake session and `read()` runs, then the load is recorded on the
  fake. Given `active` replaced with a function that raises `err` when `read()` runs, then
  `read()` raises `err` itself.
- **AT-015**: Given the `collection` on the left, when `MongoDBReader("shop", <collection>, conn,
  spark=spark).read()` runs, then the load's `collection` option is the text on the right:

  | `collection` | The `collection` option |
  |---|---|
  | `orders.2026` | `orders.2026` |
  | `a,b` | `a\,b` |
  | `*` | `\*` |
  | `logs*` | `logs\*` |
  | `a\b` | `a\\b` |
  | `\,*` | `\\\,\*` |

### Empty schema

- **AT-016**: Given a source whose `columns` is `[]`, when a full read runs, then the event log
  shows one load, an access to `columns` on the source, and one where on the source, with the
  condition `false`. `read()` returns the DataFrame that this where returned, and no attribute of
  it is accessed. No select, row access or other where is recorded.
- **AT-017**: Given a source whose `columns` is `[]`, a reader with `watermark_column="updatedAt"`
  and `schema_hints="updatedAt TIMESTAMP"`, and either `watermark_value=datetime(2026, 10, 1,
  tzinfo=UTC)` or no value, when `read()` runs, then the event log is that of AT-016: one load, an
  access to `columns` on the source, and one where on the source with the condition `false`, whose
  DataFrame `read()` returns. No select and no row access is recorded.

### Watermark read

- **AT-018**: Given `watermark_value=datetime(2026, 10, 1, tzinfo=UTC)`, when `read()` runs, then
  the event log shows, in this order:
  1. one load, with the base options; it returns the source;
  2. an access to `columns` on the source;
  3. a where on the source, with the condition `` `updatedAt` > '2026-10-01 00:00:00+00:00' ``;
  4. a select on the DataFrame that this where returned, with exactly the expressions
     `` CAST(max(`updatedAt`) AS STRING) AS upper_bound `` and
     `` typeof(max(`updatedAt`)) AS upper_bound_type ``, in this order;
  5. one `first()` on the DataFrame that this select returned;
  6. a where on the source, with the condition

     ```text
     `updatedAt` > '2026-10-01 00:00:00+00:00' AND `updatedAt` <= CAST('2026-10-21 09:30:00.5' AS timestamp)
     ```

  `read()` returns the DataFrame that the last where returned, and no attribute of it is accessed.
  No other load, where, select or row access is recorded.
- **AT-019**: Given no `watermark_value`, when `read()` runs, then the select is recorded on the
  source itself, with no where before it, and the only where on the source has the condition
  ``` `updatedAt` <= CAST('2026-10-21 09:30:00.5' AS timestamp) ```.
- **AT-020**: Given the bound query answers `(None, "timestamp")`, with or without a
  `watermark_value`, when `read()` runs, then the last where on the source has the condition
  `false`, and `read()` returns the DataFrame it returned.
- **AT-021**: Given `watermark_column="w"` and the `watermark_value` on the left, when `read()`
  runs, then the where before the select and the last where both contain `` `w` > `` followed by
  the literal on the right:

  | `watermark_value` | Literal |
  |---|---|
  | `41` | `41` |
  | `0` | `0` |
  | `-7` | `-7` |
  | `Decimal("12.50")` | `12.50BD` |
  | `Decimal("1E+3")` | `1000BD` |
  | `Decimal("-0.25")` | `-0.25BD` |
  | `"O'Brien"` | `'O\'Brien'` |
  | `r"C:\temp"` | `'C:\\temp'` |
  | `""` | `''` |
  | `date(2026, 10, 1)` | `'2026-10-01'` |
  | `datetime(2026, 10, 1, 8, 30, tzinfo=UTC)` | `'2026-10-01 08:30:00+00:00'` |
  | `datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-3)))` | `'2026-10-01 11:30:00.123456+00:00'` |

- **AT-022**: Given `watermark_column="w"`, no `watermark_value`, and the bound query answer on the
  left, when `read()` runs, then the last where's condition is the one on the right:

  | Bound query answers | Condition |
  |---|---|
  | `("12.50", "decimal(18,2)")` | ``` `w` <= CAST('12.50' AS decimal(18,2)) ``` |
  | `("9007199254740993", "bigint")` | ``` `w` <= CAST('9007199254740993' AS bigint) ``` |
  | `("it's", "string")` | ``` `w` <= CAST('it\'s' AS string) ``` |
  | `("a\\b", "string")`, whose first field is `a\b` | ``` `w` <= CAST('a\\b' AS string) ``` |

- **AT-023**: Given a reader with a watermark, when `read()` runs twice and the bound query answers
  `("2026-10-21 09:30:00.5", "timestamp")` the first time and `("2026-10-22 10:00:00", "timestamp")`
  the second, then two loads and two `first()` calls are recorded, each read's last where uses its
  own bound, and the first `read()`'s DataFrame is not the second's.
- **AT-024**: Given the `watermark_column` on the left, when a watermark read with
  `watermark_value=1` runs, then every expression the reader passes to `where` and `selectExpr`
  uses the quoted field on the right:

  | `watermark_column` | Quoted field |
  |---|---|
  | `updatedAt` | `` `updatedAt` `` |
  | `Updated At` | `` `Updated At` `` |
  | ``my`field`` | ``` `my``field` ``` |

- **AT-025**: Given a fake whose `load()` raises `err = RuntimeError("MongoTimeoutException")`,
  when `read()` runs, with or without a watermark, then it raises `err` itself, and no where or
  select is recorded. Given a bound query whose `first()` raises `err`, when `read()` runs, then
  it raises `err` itself, and no where is recorded after the select.

### In a configuration

- **AT-026**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises, when
  `ReadSource(path, connection=conn, spark=spark)` is built, then it succeeds, `active` is not
  called, and no attribute of `spark` is accessed. When
  `Process([Step("extract", [<that ReadSource>])]).run()` runs, then `state["records"]` is the
  source of the one load recorded, and the events equal those of the twin
  `MongoDBReader("shop", "orders", conn, spark=<twin session>).read()`. Given the section with
  `"watermark_column": "updatedAt"`, `"schema_hints": "updatedAt TIMESTAMP"` and
  `"options": {"sampleSize": 5000}` added, and `watermark_value=datetime(2026, 10, 1, tzinfo=UTC)`
  given to `ReadSource`, the events equal those of the twin built with the same arguments. mypy
  strict accepts `ReadSource(path, connection=conn)` with `conn` annotated `MongoDBConnection`,
  and a `MongoDBReader` bound to a variable annotated `Reader[DataFrame]`.
- **AT-027**: Given the `source` section on the left, when `ReadSource(path, connection=conn)` is
  built, then it raises `ValueError` with the message on the right:

  | `source` | Message |
  |---|---|
  | `{"system": "mongodb", "collection": "orders"}` | `<path>: source.database is required for system 'mongodb'` |
  | `{"system": "mongodb"}` | the same |
  | `{"system": "mongodb", "database": "shop"}` | `<path>: source.collection is required for system 'mongodb'` |
  | the default section with `"uri": "mongodb://db.internal"` | `<path>: source.uri is not a key of system 'mongodb'` |
  | the same with `"password"`, `"host"`, `"schema"`, `"table_name"`, `"connection"`, `"watermark_value"` or `"sampleSize"` as the key | `<path>: source.<key> is not a key of system 'mongodb'` |
  | the default section with `"database": 1` | `<path>: source.database must be a string` |
  | the default section with `"collection": null` | `<path>: source.collection must be a string` |
  | the default section with `"schema_hints": ["updatedAt TIMESTAMP"]` | `<path>: source.schema_hints must be a string` |
  | the default section with `"watermark_column": true` | `<path>: source.watermark_column must be a string` |
  | the default section with `"options": {"sampleSize": [1]}` | `<path>: source.options must be an object of strings, numbers and booleans` |

- **AT-028**: When `ReadSource(path)` is built, then it raises
  `ValueError("<path>: source system 'mongodb' needs the argument connection")`. When
  `ReadSource(path, connection=conn, client=object())` or `ReadSource(path, client=object())` is
  built, then it raises `ValueError("<path>: source system 'mongodb' takes no argument client")`.
  `ReadSource(path, connection=conn, client=None, watermark_value=None, spark=None)` builds.
- **AT-029**: Given the callback, the section and the `connection` on the left, when the callback
  is built, then it raises `ValueError` with the message on the right:

  | Callback | Section | `connection` | Message |
  |---|---|---|---|
  | `ReadSource` | the default `source` | `PostgresConnection("db.internal", "shop", "ingest", "pw")` | `<path>: source system 'mongodb' needs a MongoDBConnection as the argument connection, not PostgresConnection` |
  | `ReadSource` | the default `source` | `URI`, the string | `<path>: source system 'mongodb' needs a MongoDBConnection as the argument connection, not str` |
  | `ReadSource` | `{"system": "postgres", "table_name": "public.orders"}` as `source` | `conn` | `<path>: source system 'postgres' needs a PostgresConnection as the argument connection, not MongoDBConnection` |
  | `AppendOnly`, `FullLoad` | `{"system": "postgres", "table_name": "public.orders"}` as `target` | `conn` | `<path>: target system 'postgres' needs a PostgresConnection as the argument connection, not MongoDBConnection` |
  | `SCD2` | the same `target`, with `"scd2": {"key_columns": ["id"], "effective_time_column": "updated_at"}` | `conn` | the same |

  A `ReadSource` given an instance of a subclass of `MongoDBConnection` builds.
- **AT-030**: Given `{"target": {"system": "mongodb", "database": "shop", "collection": "orders"}}`,
  when `AppendOnly(path)` or `FullLoad(path)` is built, then it raises
  `ValueError("<path>: target.system 'mongodb' is not one of: bigquery, disk, gcs, json_lines, mysql, postgres, s3, unity_catalog, volume")`.
  Given a `source` section whose `system` is `"MongoDB"` or `"mongo"`, when `ReadSource(path)` is
  built, then it raises
  `ValueError("<path>: source.system '<name>' is not one of: bigquery, disk, gcs, json_api, mongodb, mysql, mysql_cdc, postgres, s3, unity_catalog, volume")`.
- **AT-031**: Given the default section with the key on the left added, when
  `ReadSource(path, connection=conn)` is built, then it raises the `ValueError` that
  `MongoDBReader` raises, with the message on the right and exactly one note,
  `while building the source of <path>, system 'mongodb'`:

  | Added key | Message |
  |---|---|
  | `"schema_hints": " "` | `schema_hints must not be empty` |
  | `"schema_hints": "{\"type\": \"struct\"}"` | `schema_hints must be a DDL string, not JSON` |
  | `"options": {"database": "other"}` | `option 'database' is set by the connector` |

### Logging and secrets

- **AT-032**: Given `caplog` capturing every level, when connections, readers and `ReadSource`
  callbacks are built, and `read()` runs as a full read, as a watermark read, with a bound of
  `None` and over a source with no columns, then no log record is captured.
- **AT-033**: Given the connection strings of AT-003, AT-004, AT-009 and AT-029, then neither
  `u53r-tok` nor `p455-tok` appears in any `repr` or `str` of the connections and readers those
  criteria build, nor in the text or the notes of any error those criteria raise.

## Decisions

Taken by the maintainer on 2026-10-24, in conversation. Their rationale is the technical reason the
maintainer gave, or a fact of the connector's source (F1 to F5) where the maintainer named one;
where no reason was given, it is `TODO(maintainer)`.

1. **Scope: the reader only.** One connector class, the reader. No writer in this spec: a MongoDB
   writer, and its atomicity question, would be a separate spec later.
   - Rationale: `TODO(maintainer)`.
2. **Engine: Spark, through the MongoDB Spark Connector,** a prerequisite JAR, as the PostgreSQL
   JDBC driver is.
   - Rationale: `schemaHints` is a read option of the MongoDB Spark Connector, and every shipped
     connector except the JSON ones reads into a Spark DataFrame (backlog, "Shared by ideas 8 and
     9").
3. **Credentials: the connection string is passed at run time,** not written in the configuration
   file. It reaches `ReadSource` wrapped in a new public class, `MongoDBConnection(uri)` (named by
   decision 6), through the existing `connection` argument, which widens from
   `PostgresConnection | None` to `PostgresConnection | MongoDBConnection | None` on `ReadSource`.
   `AppendOnly`, `FullLoad` and `SCD2` keep `PostgresConnection`, since there is no MongoDB
   writer. Like `PostgresConnection`, `MongoDBConnection` checks the scheme, `mongodb://` or
   `mongodb+srv://`, when it is built, and its `repr` never shows the user or the password:
   `MongoDBConnection(hosts='cluster0.example.net')`. It is exported from
   `ingestio.connector.all`. The usage the maintainer approved:

   ```python
   from ingestio.connector.all import MongoDBConnection
   uri = dbutils.secrets.get("mongo", "uri")
   ReadSource("job.json", connection=MongoDBConnection(uri))
   ```

   - Rationale: the maintainer's technical specification: the job reads the connection string
     from a secret store at run time and hands it to the connector. `AppendOnly`, `FullLoad` and
     `SCD2` keep `PostgresConnection` because there is no MongoDB writer. Why the existing
     `connection` argument widens, rather than a new argument: `TODO(maintainer)`.
4. **Schema: only `schema_hints`, a DDL string.** A new configuration key `schema_hints` of the
   MongoDB source section, a DDL string such as `"updatedAt TIMESTAMP, total DECIMAL(18,2)"`,
   passed to the connector's `schemaHints` read option. Spark infers the other fields by sampling;
   the sample size is set through `options` (`sampleSize`). Not offered: a full `schema` key, and
   the JSON form of the hints.
   - Rationale for the DDL form: the `schema` key of the other systems is a DDL string
     ([Configuration](../docs/concepts/configuration.md)). Why hints only, and no full `schema`
     key, taken from the chosen option's description, for the maintainer's review: it keeps a
     single schema key, where `schema_hints` and `schema` together would be one more key to test
     and document.
5. **Watermark: `watermark_column`,** a field such as `updatedAt`, with the same recipe as the
   other readers: read the maximum, then filter between the two bounds, as `PostgresReader` and
   `UnityCatalogReader` do, the filter pushed down into MongoDB as a `$match` by the Spark
   connector's filter pushdown. Not offered: the time inside the `_id` ObjectId.
   - Rationale, taken from the chosen option's description, for the maintainer's review: it is
     the same recipe as the other readers (read the maximum, filter between the two bounds),
     pushed to MongoDB as a `$match`, while a watermark from the time inside the ObjectId needs a
     special rule in the reader.
6. **Names: `MongoDBReader`, the system `mongodb`, the module `src/ingestio/connector/mongodb.py`,
   and the connection class `MongoDBConnection`** (first draft's question 1, option (b)). The
   option the maintainer chose renamed the connection class with the reader. The slug stays
   `mongodb_reader`.
   - Rationale, confirmed by the maintainer: the product's name read as one word, as
     `BigQueryReader` gives `bigquery`.
7. **Keys: two required keys, `database` and `collection`** (question 2, option (a)).
   - Rationale, taken from the chosen option's description, for the maintainer's review: they are
     the connector's own two options, passed as they are, and a collection name may hold a `.`
     (`orders.2026`), so no rule splits anything.
8. **The path of the connection string: no check, documented** (question 3, a new option). The
   path is accepted whatever it names and never rewritten; the documentation says that it stays
   the driver's default authentication database, and that the connector's documentation gives the
   connection string precedence while its code gives it to the explicit options.
   - Rationale: F2, explicit `database` and `collection` beat the path, and the reader always sets
     both.
9. **Collection names are escaped** (a question the main session raised). The reader writes a `\`
   before each `\`, `,` and `*` of `collection` before it passes the option, so that every
   collection name reaches the connector as one collection. The `repr` and the error messages show
   the name as given.
   - Rationale: F5, the connector reads `collection` as a list, with `*` alone for every
     collection and `\` as the escape.
10. **The callbacks check the type of `connection`** (question 4, option (a)), for every system
    that takes it, in `ReadSource`, `AppendOnly`, `FullLoad` and `SCD2`.
    - Rationale, taken from the chosen option's description, for the maintainer's review: the
      callbacks raise a `ValueError` when they are built, with the file's path and the system in
      the message, and no existing test breaks; checking in the connectors would change two
      validated connectors, and no check fails only when the process runs, with an
      `AttributeError`.
11. **The bound is a Spark aggregate** (question 5, option (a)), as `UnityCatalogReader` takes it.
    No native client and no extra.
    - Rationale, taken from the chosen option's description, for the maintainer's review: with no
      native client there is no extra, so the core and the extras stay as they are
      ([ADR 0002](../docs/decisions/0002-zero-dependency-core.md)).
12. **The watermark values are those of the other readers** (question 6, option (a)): `int`, finite
    `Decimal`, `str`, `date` and aware `datetime`.
    - Rationale, taken from the chosen option's description, for the maintainer's review: they
      are the values and the literals of the other readers, as `UnityCatalogReader` writes them.
13. **The reader converts nothing, and `watermark_column` is one top-level field** (question 7,
    option (a)). Hint names may be dotted paths into nested documents, which the connector handles
    (F3); the reader passes them as given and claims nothing beyond F3.
    - Rationale, taken from the chosen option's description, for the maintainer's review:
      converting `_id` or flattening nested documents is formatting, which belongs to callbacks,
      not a format choice inside a connector. The watermark field is top-level because whether the
      connector pushes a nested comparison is not established, and a top-level name that holds a
      `.` could no longer be named. The note on hint names rests on F3.
14. **A `schema_hints` whose first character after leading whitespace is `{` is rejected** with
    `schema_hints must be a DDL string, not JSON` (question 8, option (a)).
    - Rationale: it is the rule by which the connector itself takes the text as JSON (F3).
15. **The minimum connector release is 10.4.0, for Spark 3.5** (Databricks Runtime 15.4 LTS), and
    the 11.x line for Spark 4 (question 9, settled by F1). Support for Spark 4.2 is not
    established. No check at run time.
    - Rationale: F1, 10.4.0 is the first release that reads `schemaHints`. Why no check at run
      time, taken from the chosen option's description, for the maintainer's review: a check would
      cost one more analysis of the DataFrame per read (a round trip on Spark Connect) and a parse
      of the hints.
16. **Reserved options** (question 10, option (a)): `connection.uri`, `database`, `collection`,
    `schemaHints` and `aggregation.pipeline`, in any case, alone or after `spark.mongodb.read.` or
    `spark.mongodb.`.
    - Rationale for the forms after `spark.mongodb.read.`: F2, `withOverrides` keeps such a key as
      it is, and it would then compete with the reader's own. For the forms after
      `spark.mongodb.`: `MongoConfig.PREFIX` is `"spark.mongodb."` (`MongoConfig.java`,
      `r11.1.0`), and `AbstractMongoConfig.createUsageOptions` reads the keys under that prefix as
      the global defaults of the read scope, beside those under `spark.mongodb.read.` (`r10.4.0`
      and `r11.1.0`).
17. **The connection string in Spark's plans is documented** (question 11, option (a)): it reaches
    Spark as the `connection.uri` option, and the documentation shows how a job adds `uri` to
    `spark.sql.redaction.options.regex`. The reader sets no session configuration.
    - Rationale, taken from the chosen option's description, for the maintainer's review: the
      reader sets no session configuration, and decision 3 stands, where the alternative reopened
      it.
18. **No integration tests** (question 12, option (d)). Every criterion runs with the test
    doubles; what only a real run could check stays under Known limitations as not verified,
    except what F1 to F5 establish.
    - Rationale: `TODO(maintainer)`.
19. **An empty or missing collection reads as empty, documented** (a question the main session
    raised). When the source's schema has no field (F4), `read()` returns the source filtered with
    `false`, a DataFrame with no columns and no rows, and runs no bound query, with or without
    `watermark_column`. The documentation says that the table writers reject a DataFrame with no
    columns, so a job that appends an empty first read fails there.
    - Rationale, taken from the chosen option's description, for the maintainer's review: `read()`
      returns a DataFrame with no columns and no rows, with no bound query, the same as the full
      read, while raising a `ValueError` would make a first run on an empty collection fail the
      pipeline. That an empty or missing collection gives a schema with no field, the hints
      included, is F4.

Proposed by this spec, for the maintainer's approval. Each follows a precedent, named with it, or
gives its technical reason.

20. **Building does nothing but validate.** Each `read()` looks up the session and loads the
    collection again, so that a reader built once serves every run that calls it, as the
    PostgreSQL spec (decision 18) and the Unity Catalog spec (decision 6) do.
21. **The filter is Spark SQL text,** passed to `DataFrame.where` and `DataFrame.selectExpr`, as in
    the Unity Catalog spec (decision 7). Spark pushes it to the connector as it pushes Column
    expressions, and tests observe it without a JVM.
22. **Literals and the bound are `UnityCatalogReader`'s** (its decisions 8 and 9), under decisions
    11 and 12: numbers as numeric literals, the rest as string literals that Spark casts to the
    field's type, aware datetimes in UTC, and the bound as Spark's own text of the maximum cast
    back to the type Spark reports, so that it never passes through Python types.
23. **The comparison is strictly greater (`>`), and `null` or missing watermarks are never read,**
    first run included, as in the PostgreSQL spec (decision 23).
24. **The captured bound is not exposed,** and the reader keeps no state between calls, as the
    other readers.
25. **`MongoDBConnection` reads only the scheme and the authority, and rejects what it could not
    show safely** (an `@` after the hosts). This is what lets its `repr` show the hosts and never
    the user or the password, the property of decision 3. Its error messages are fixed texts that
    hold nothing of the string. With the check of the path gone (decision 8), it no longer reads
    the path, decodes it or splits it into a database and a collection, as the first draft did.
26. **The connection string reaches Spark as given, never rewritten.** Rewriting it means parsing
    its options and its escapes, which is the driver's work, and decision 8 makes it unneeded.
27. **The reader never parses `schema_hints`** beyond the checks of decisions 14 and 28. Parsing
    DDL needs Spark, and building the reader touches no Spark (decision 20); the connector parses
    the hints when the reader reads.
28. **A blank `schema_hints` is rejected like an empty one,** with `schema_hints must not be
    empty`, the blanks being the characters that Java's `String.trim()` removes. The connector
    trims the text and treats a blank one as no hints (F3), so a blank key would silently mean no
    hints, which leaving the key out already says.
29. **An empty schema gives `where(false)` in a full read too, and the reader reads
    `source.columns` to know.** One rule for both modes: the DataFrame holds no row at any action.
    Without it, the source of a full read, which is not frozen, could show documents inserted
    after `read()` as rows with no columns on classic Spark, or with a schema inferred anew on
    Spark Connect. Reading `columns` runs no Spark job on classic Spark, where `load()` already
    inferred the schema; on Spark Connect it costs one analysis request, which samples the
    collection, at every `read()`.
30. **The escape of decision 9 is one pass over the name,** each `\`, `,` and `*` preceded by a
    `\`. Every `*` is escaped, not only a name that is `*` alone: `\*` is a valid escape (F5), and
    one rule is simpler than two.
31. **Options pass through, with the reader's own keys reserved,** as for the PostgreSQL, Unity
    Catalog and BigQuery readers, with the message they use.
32. **Errors propagate unchanged, and the reader writes no log lines,** as in the Unity Catalog
    spec (decision 19).
33. **No extra:** the reader imports only pyspark, lazily, and the connector is a JAR that the
    runtime provides, as Delta Lake is for the Unity Catalog connectors (rule 4 of the extras
    pattern).

### ADRs

- **No decision changes the philosophy, and none closes the door to streaming.** No new ADR is
  needed.
- **ADR 0012, amended.** The reader leans towards batch: `read()` returns one slice, with a
  watermark fixed when the reader is built. A streaming or CDC source for MongoDB, such as change
  streams through the connector's streaming read, tracks a resume token and would be a different
  connector. This goes in the "What leans towards batch" list, as for the other connectors; the
  doc-keeper writes it.
- **ADR 0013, amended.** Its convention applies unchanged: `database`, `collection`,
  `watermark_column`, `schema_hints` and `options` are configuration keys, the credentials stay
  out of the configuration in `MongoDBConnection`, and `aggregation.pipeline` is reserved under
  its consequence that a reader filtering inside the source offers no free-form query. One more
  consequence: the `$match` that the connector pushes down is how `MongoDBReader` filters inside
  the source. The doc-keeper writes it.
- **ADR 0015, amended.** Its Decision lists the system names, which gain `mongodb`. Its
  consequences can record that the callbacks' `connection` argument takes the connection value of
  the system that needs one, widened per system, which later systems with credentials (backlog
  idea 8) would follow, and that the callbacks check its type (decision 10). The doc-keeper writes
  both, for the maintainer's approval.
- **ADR 0002 applies unchanged.** There is no extra; the connector is a prerequisite JAR.

## Open questions

None open. On 2026-10-24 the maintainer answered the twelve questions of the first draft and two
that the main session raised. The answers are decisions 6 to 19:

| Question | Answer | Decision |
|---|---|---|
| 1. Class names, system name and module | (b), with the connection class renamed `MongoDBConnection` | 6 |
| 2. The keys that name the collection | (a) `database` and `collection` | 7 |
| 3. A connection string that names a database or a collection | a new option: no check, documented | 8 |
| Collection names and the connector's list syntax | escape `\`, `,` and `*` | 9 |
| 4. The type of `connection` | (a) the callbacks check it | 10 |
| 5. The upper bound, and the extra | (a) a Spark aggregate, no extra | 11 |
| 6. The watermark values | (a) the five types of the other readers | 12 |
| 7. `_id`, nested documents and the watermark field | (a) nothing converted, a top-level field | 13 |
| 8. The form of `schema_hints` | (a) reject the JSON form | 14 |
| 9. Connector versions | settled by F1: 10.4.0 for Spark 3.5, 11.x for Spark 4 | 15 |
| 10. Reserved options | (a) | 16 |
| 11. The connection string in Spark's plans | (a) documented | 17 |
| 12. Integration tests and CI | (d) none | 18 |
| An empty or missing collection | return an empty DataFrame, documented | 19 |

For the maintainer's review: decisions 1 and 18 carry `TODO(maintainer)` as their reason, and
decision 3 for the widening of `connection` rather than a new argument.

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-24 | planning | draft | Drafted from the maintainer's five decisions of 2026-10-24 (reader only, Spark engine, `MongoConnection(uri)` through `connection`, `schema_hints` as DDL, `watermark_column`): `MongoConnection`, `MongoReader` with `UnityCatalogReader`'s watermark recipe, the `mongo` source system and a check of the connection's type in the four callbacks. 30 criteria without Spark, 7 opt-in integration scenarios, 12 open questions. |
| 2026-10-24 | planning | draft | Revised with the maintainer's answers to the twelve questions and two the main session raised, and with the connector-source facts F1–F5: renamed to `MongoDBReader`/`MongoDBConnection`/`mongodb`, no check of the connection string's path (F2), `collection` escaped (F5), an empty schema reads as `where(false)` (F4), blank and JSON hints rejected (F3), minimum connector 10.4.0 (F1), no integration tests. 33 criteria with test doubles, 33 decisions, no open question. |
| 2026-10-24 | planning | draft | F3 and F4 cite `r10.4.0` as well as `r11.1.0`, which the main session diffed as identical; the version gap is closed. Citations only. |
| 2026-10-24 | planning | draft | Rationale lines only: the `TODO(maintainer)` of decisions 4, 5, 7, 10–13, 15, 17 and 19 hold the reasons of the options the maintainer chose, marked for review; decision 16 cites `MongoConfig.PREFIX`; decision 6's reason confirmed. Decisions 1, 18 and 3 (the widening) keep `TODO(maintainer)`. |
| 2026-10-24 | approval | approved | The maintainer approved the spec on condition that the last revision change only the rationale lines; the main session checked the diff and set `approved`. |
| 2026-10-24 | tests | tested | 154 cases in 48 functions in `tests/test_mongodb_reader.py` cover AT-001 to AT-033 with test doubles only; reading `columns` is an event of the doubles' log. `SOURCE_NAMES` in `tests/test_config_connectors.py` gains `mongodb`. The new tests fail on the missing `MongoDBConnection`, `MongoDBReader` and `mongodb` row; the AT-030 target cases pass by design. The test-writer checked the tests against an in-memory stand-in run from stdin, writing no file. It found a gap: no criterion has two `@` in the authority, so splitting the hosts at the first `@` would pass. |
| 2026-10-24 | return to planning | draft | Returned by the main session for the test-writer's gap: AT-003 gains an example whose authority holds two `@`. The maintainer pre-approved a revision limited to that. |
| 2026-10-24 | planning | draft | Added `mongodb://u53r-tok:p4@p455-tok@db.internal/?authSource=admin` (hosts `db.internal`) to the examples that AT-003 iterates over, so that splitting the authority at the first `@` fails AT-003 and AT-033. One row only. |
| 2026-10-24 | approval | approved | Under the maintainer's pre-approval: the main session checked that the diff is that one row and set `approved`. |
| 2026-10-24 | tests | tested | AT-003 gains the case `two-ats`, which AT-033 also checks. `tests/test_mongodb_reader.py` has 155 cases in 48 functions: 32 fail and 121 error at setup, all on the missing feature, and 2 pass by design (the AT-030 target cases). Against the in-memory stand-in, the first-`@` mutation fails AT-003 and AT-033. |
| 2026-10-24 | documentation | documented | Documented `MongoDBConnection` and `MongoDBReader` on the connectors page, and the `mongodb` system and the new type check of `connection` on the configuration and callbacks pages. Amended ADRs 0012, 0013 and 0015, with `TODO(maintainer)` left open for the absence of a writer and for widening `connection`. Updated `docs/index.md` and the architecture kb entry. |
| 2026-10-24 | implementation | implemented | Added `MongoDBConnection` and `MongoDBReader` in `connector/mongodb.py`, exported from `ingestio.connector.all`, and the `mongodb` source system of the closed table. `ReadSource` takes a `MongoDBConnection`, and `_build` checks the type of `connection` for every system that takes one (a `connection_type` field of `_System`). Suite: 1700 passed, 123 skipped (existing opt-in tests); ruff and mypy strict clean. |
| 2026-10-24 | validation | validated | AT-001 to AT-033 pass in `tests/test_mongodb_reader.py` (155 cases, no opt-in criteria, no Spark run). The suite gives 1700 passed and 123 skipped (existing opt-in tests), the docs examples pass, and ruff, format, mypy strict and the lock check are clean. A static review of `read()`'s Spark calls finds one action, the bound query, and the `connection` type check breaks no existing test. Non-blocking notes: a duck-typed connection with a bad `uri` raises `StopIteration`, and a connection string with no `@` shows its whole authority in `repr`. |
| 2026-10-24 | amendment | validated | AT-030 amended by the main session after the validation of [mysql_connectors](mysql_connectors.md), approved on 2026-10-24: `mysql` becomes a source and a target system there, so both messages of AT-030 list `mysql`. What the criterion checks is unchanged; `tests/test_mongodb_reader.py` already uses these lists, updated by the test-writer of `mysql_connectors`. The lists under Behaviour (Lists of systems) record the state when this spec landed. |
| 2026-10-24 | amendment | validated | AT-030 amended by the main session, as [mysql_cdc](mysql_cdc.md) (What becomes outdated) asks, approved on 2026-10-24: `mysql_cdc` becomes a source system there, so the source message of AT-030 lists it. What the criterion checks is unchanged; the test-writer of `mysql_cdc` updates `SOURCE_NAMES` in `tests/test_mongodb_reader.py`. |
