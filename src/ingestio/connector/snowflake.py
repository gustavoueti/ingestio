"""Snowflake connectors built on the Snowflake Connector for Spark.

`SnowflakeReader` returns a Spark DataFrame and filters inside Snowflake. `SnowflakeWriter`
appends a DataFrame to a table, or replaces the table's rows with it, in the connector's own
transaction. pyspark is imported lazily, so this module imports without it. The connector's JAR
and the Snowflake JDBC driver are prerequisites that the runtime provides.
"""

from __future__ import annotations

import base64
import re
from collections.abc import Mapping
from dataclasses import KW_ONLY, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

__all__ = ["SnowflakeConnection", "SnowflakeReader", "SnowflakeWriter"]

_FORMAT = "snowflake"

# Options that carry the connection's values or credentials, or that the connector sets.
_RESERVED = frozenset(
    {
        "sfurl",
        "sfdatabase",
        "sfschema",
        "sfwarehouse",
        "sfrole",
        "sfuser",
        "sfpassword",
        "pem_private_key",
        "sfauthenticator",
        "sftoken",
        "sfaccount",
        "sfworkloadidentityprovider",
        "oauthclientid",
        "oauthclientsecret",
        "oauthtokenrequesturl",
        "oauthscope",
        "connection_id",
        "dbtable",
        "query",
    }
)
# Options that the writer's transaction and its guarantee of a missing target depend on.
_WRITER_RESERVED = _RESERVED | {
    "usestagingtable",
    "truncate_table",
    "column_mapping",
    "columnmap",
    "preactions",
    "postactions",
    "internal_skip_write_when_writing_empty_dataframe",
}

_PART = r'(?:[A-Za-z_][A-Za-z0-9_$]*|"(?:[^"]|"")+")'
_TABLE = re.compile(rf"{_PART}(?:\.{_PART}){{0,2}}")
_IDENTIFIER = re.compile(_PART)
_NUMBER = re.compile(r"[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?")

_BEGIN = "-----BEGIN PRIVATE KEY-----"
_END = "-----END PRIVATE KEY-----"
_KEY_FORM = "private_key must be an unencrypted PKCS#8 key, as PEM text or its base64 body"

_Options: TypeAlias = Mapping[str, str | int | float | bool]
_Watermark: TypeAlias = int | Decimal | str | date


def _key_body(text: str) -> str:
    """The base64 body of a private key given as PEM text or as its body; raise if it is not."""
    text = text.strip()
    if text.startswith("-----BEGIN "):
        lines = text.splitlines()
        if lines[0] == "-----BEGIN ENCRYPTED PRIVATE KEY-----":
            raise ValueError("private_key must be unencrypted")
        if lines[0] == "-----BEGIN RSA PRIVATE KEY-----":
            raise ValueError(
                "private_key must be PKCS#8 (BEGIN PRIVATE KEY), not PKCS#1 (BEGIN RSA PRIVATE KEY)"
            )
        if len(lines) < 2 or lines[0] != _BEGIN or lines[-1] != _END:
            raise ValueError(_KEY_FORM)
        text = "\n".join(lines[1:-1])
    body = re.sub(r"\s+", "", text)
    if not body:
        raise ValueError(_KEY_FORM)
    try:
        base64.b64decode(body, validate=True)
    except ValueError:
        raise ValueError(_KEY_FORM) from None
    return body


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

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("host must not be empty")
        if not self.database:
            raise ValueError("database must not be empty")
        for name in ("user", "schema", "warehouse", "role"):
            if getattr(self, name) == "":
                raise ValueError(f"{name} must not be empty")
        if self.private_key is None and self.oauth_token is None:
            raise ValueError("give private_key or oauth_token")
        if self.private_key is not None and self.oauth_token is not None:
            raise ValueError("give private_key or oauth_token, not both")
        if self.oauth_token == "":
            raise ValueError("oauth_token must not be empty")
        if self.private_key is not None:
            if self.user is None:
                raise ValueError("private_key needs user")
            _key_body(self.private_key)

    def __repr__(self) -> str:
        return (
            f"SnowflakeConnection(host={self.host!r}, database={self.database!r}, "
            f"schema={self.schema!r}, warehouse={self.warehouse!r}, role={self.role!r}, "
            f"user={self.user!r})"
        )

    def _spark_options(self) -> dict[str, str | int | float | bool]:
        """The options of the Snowflake Connector for Spark that every load and save carries."""
        options: dict[str, str | int | float | bool] = {
            "sfURL": self.host,
            "sfDatabase": self.database,
        }
        if self.schema is not None:
            options["sfSchema"] = self.schema
        if self.warehouse is not None:
            options["sfWarehouse"] = self.warehouse
        if self.role is not None:
            options["sfRole"] = self.role
        if self.user is not None:
            options["sfUser"] = self.user
        if self.private_key is not None:
            options["pem_private_key"] = _key_body(self.private_key)
        if self.oauth_token is not None:
            options["sfAuthenticator"] = "oauth"
            options["sfToken"] = self.oauth_token
        return options


def _check_table(table_name: str) -> None:
    if not _TABLE.fullmatch(table_name):
        raise ValueError(
            "table_name must be 'table', 'schema.table' or 'database.schema.table', "
            "each part a Snowflake identifier"
        )


def _check_column(column: str | None) -> None:
    if column is None:
        return
    if column == "":
        raise ValueError("watermark_column must not be empty")
    if not _IDENTIFIER.fullmatch(column):
        raise ValueError("watermark_column must be a Snowflake identifier")


def _check_options(
    options: _Options | None, reserved: frozenset[str]
) -> dict[str, str | int | float | bool]:
    given = dict(options or {})
    for key in given:
        if key.lower() in reserved:
            raise ValueError(f"option '{key}' is set by the connector")
    return given


def _q(text: str) -> str:
    """A single-quoted string constant: `\\` and `'` escaped."""
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


def _watermark_literal(value: _Watermark) -> str:
    """Validate a watermark value and turn it into a SQL literal."""
    if isinstance(value, bool):
        raise ValueError("unsupported watermark_value type: bool")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("watermark_value must be finite")
        return format(value, "f")
    if isinstance(value, str):
        return _q(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("watermark_value must be timezone-aware")
        return _q(value.astimezone(UTC).isoformat())
    if isinstance(value, date):
        return _q(value.isoformat())
    raise ValueError(f"unsupported watermark_value type: {type(value).__name__}")


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
    ) -> None:
        _check_table(table_name)
        _check_column(watermark_column)
        if watermark_value is not None and watermark_column is None:
            raise ValueError("watermark_value needs watermark_column")
        self._value = None if watermark_value is None else _watermark_literal(watermark_value)
        self._options = _check_options(options, _RESERVED)
        self.table_name = table_name
        self.connection = connection
        self.watermark_column = watermark_column
        self.watermark_value = watermark_value
        self.options = options
        self.spark = spark

    def read(self) -> DataFrame:
        """Return a DataFrame over the rows the job needs."""
        spark = self.spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.active()
        base = self.connection._spark_options()
        table = self.table_name
        column = self.watermark_column
        if column is None:
            return self._load(spark, {**base, "dbtable": table})

        from pyspark.sql.types import DateType, NumericType, TimestampNTZType, TimestampType

        type_frame = self._load(
            spark, {**base, "query": f"SELECT {column} FROM {table} WHERE FALSE"}
        )
        watermark_type = type_frame.schema.fields[0].dataType

        if isinstance(watermark_type, DateType):
            render = f"TO_VARCHAR(MAX({column}), 'YYYY-MM-DD')"
        elif isinstance(watermark_type, TimestampType | TimestampNTZType):
            render = f"TO_VARCHAR(MAX({column}), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM')"
        else:
            render = f"TO_VARCHAR(MAX({column}))"
        lower = f"{column} > {self._value}" if self._value is not None else None
        query = f"SELECT {render} AS UPPER_BOUND FROM {table}"
        if lower is not None:
            query += f" WHERE {lower}"
        row = self._load(spark, {**base, "query": query}).first()
        upper = None if row is None else row[0]

        if upper is None:
            predicate = "FALSE"
        else:
            text = str(upper)
            numeric = isinstance(watermark_type, NumericType) and _NUMBER.fullmatch(text)
            upper_predicate = f"{column} <= {text if numeric else _q(text)}"
            predicate = upper_predicate if lower is None else f"{lower} AND {upper_predicate}"
        return self._load(spark, {**base, "query": f"SELECT * FROM {table} WHERE {predicate}"})

    def _load(self, spark: SparkSession, options: _Options) -> DataFrame:
        merged = {**options, **self._options}
        return spark.read.format(_FORMAT).options(**merged).load()

    def __repr__(self) -> str:
        return (
            f"SnowflakeReader(table_name={self.table_name!r}, "
            f"host={self.connection.host!r}, database={self.connection.database!r})"
        )


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
    ) -> None:
        _check_table(table_name)
        self._options = _check_options(options, _WRITER_RESERVED)
        self.table_name = table_name
        self.connection = connection
        self.options = options

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` to the target, in one transaction.

        Args:
            data: The rows to write.
            replace: Remove every row of the target first, in the same transaction, so that
                the target ends up holding exactly the rows of `data`. Appends when `False`.
        """
        if not data.columns:
            raise ValueError("the DataFrame has no columns")
        options: dict[str, str | int | float | bool] = {
            **self.connection._spark_options(),
            "dbtable": self.table_name,
            "usestagingtable": "off",
            "truncate_table": "on",
            "column_mapping": "name",
        }
        if replace:
            options["preactions"] = "TRUNCATE TABLE %s"
        options.update(self._options)
        data.write.format(_FORMAT).options(**options).mode("append").save()

    def __repr__(self) -> str:
        return (
            f"SnowflakeWriter(table_name={self.table_name!r}, "
            f"host={self.connection.host!r}, database={self.connection.database!r})"
        )
