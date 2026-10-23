"""BigQuery connectors built on Spark's BigQuery data source.

`BigQueryReader` returns a Spark DataFrame and filters inside BigQuery. `BigQueryWriter` appends a
DataFrame to a table, or replaces the table's rows with it, through a staging table and one
BigQuery statement. pyspark and google-cloud-bigquery are imported lazily, so this module imports
without either.
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from google.cloud.bigquery import Client, SchemaField
    from pyspark.sql import DataFrame, SparkSession

__all__ = ["BigQueryReader", "BigQueryWriter"]

_RESERVED = (
    "table",
    "path",
    "project",
    "dataset",
    "parentproject",
    "credentials",
    "credentialsfile",
    "gcpaccesstoken",
    "gcpaccesstokenprovider",
)
_READER_RESERVED = (*_RESERVED, "filter", "query", "snapshottimemillis")
_WRITER_RESERVED = (*_RESERVED, "writeatleastonce")

_Options: TypeAlias = Mapping[str, str | int | float | bool]
_Watermark: TypeAlias = int | Decimal | str | date


def _split_table(table_name: str) -> tuple[str, str, str]:
    parts = table_name.split(".")
    if len(parts) != 3 or not all(parts):
        raise ValueError("table_name must be 'project.dataset.table'")
    return parts[0], parts[1], parts[2]


def _quote(name: str) -> str:
    """Quote an identifier: wrap it in backticks and escape backslashes and backticks."""
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def _quote_table(parts: tuple[str, ...]) -> str:
    return ".".join(_quote(part) for part in parts)


def _string_literal(text: str) -> str:
    """A GoogleSQL string literal: escapes are backslashes, not doubled quotes."""
    escaped = text.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
    return "'" + escaped.replace("\r", "\\r") + "'"


def _check_billing_project(billing_project: str | None) -> None:
    if billing_project == "":
        raise ValueError("billing_project must not be empty")


def _check_options(
    options: _Options | None, reserved: tuple[str, ...]
) -> dict[str, str | int | float | bool]:
    given: dict[str, str | int | float | bool] = dict(options or {})
    for key in given:
        if key.lower() in reserved:
            raise ValueError(f"option '{key}' is set by the connector")
    return given


def _merge(defaults: _Options, user: _Options) -> dict[str, str | int | float | bool]:
    """`defaults` with the user's options replacing the same names, in any case."""
    names = {key.lower() for key in user}
    merged = {key: value for key, value in defaults.items() if key.lower() not in names}
    merged.update(user)
    return merged


def _watermark_literal(value: _Watermark) -> str:
    """Validate a watermark value and turn it into a GoogleSQL literal."""
    if isinstance(value, bool):
        raise ValueError("unsupported watermark_value type: bool")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("watermark_value must be finite")
        return f"BIGNUMERIC '{format(value, 'f')}'"
    if isinstance(value, str):
        return _string_literal(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("watermark_value must be timezone-aware")
        return f"TIMESTAMP '{value.astimezone(UTC).isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    raise ValueError(f"unsupported watermark_value type: {type(value).__name__}")


def _new_client(billing_project: str | None) -> Client:
    from google.cloud import bigquery

    return bigquery.Client(project=billing_project)


def _close(client: Client) -> None:
    client.close()  # type: ignore[no-untyped-call]


def _require_bigquery(connector: str) -> None:
    try:
        from google.cloud import bigquery  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            f"{connector} needs google-cloud-bigquery: pip install 'ingestio[bigquery]'"
        ) from exc


def _staging_field(name: str, target: SchemaField) -> SchemaField:
    """A field like the target's, as nullable as the load needs, with no extras."""
    from google.cloud.bigquery import SchemaField

    nested = tuple(_staging_field(child.name, child) for child in target.fields)
    return SchemaField(
        name,
        target.field_type,
        mode="REPEATED" if target.mode == "REPEATED" else "NULLABLE",
        fields=nested,
        max_length=target.max_length,
        precision=target.precision,
        scale=target.scale,
    )


class BigQueryReader:
    """Reads a BigQuery table into a Spark DataFrame, filtering inside BigQuery.

    Args:
        table_name: Source table, `project.dataset.table`. The configuration key of the same
            name.
        watermark_column: Column that orders changes. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only rows above it are read.
            Requires `watermark_column`.
        billing_project: Project billed for the read and for the bound query. The default
            project of the environment's credentials when omitted.
        options: Options of Spark's BigQuery data source passed through, such as
            `maxParallelism` or `readDataFormat`.
        client: BigQuery client for the bound query. A new one is created at each watermark
            read when omitted.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
        *,
        watermark_column: str | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        billing_project: str | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        client: Client | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._parts = _split_table(table_name)
        _check_billing_project(billing_project)
        self._options = _check_options(options, _READER_RESERVED)
        if watermark_column == "":
            raise ValueError("watermark_column must not be empty")
        if watermark_value is not None and watermark_column is None:
            raise ValueError("watermark_value needs watermark_column")
        self._value = None if watermark_value is None else _watermark_literal(watermark_value)
        if watermark_column is not None:
            _require_bigquery("BigQueryReader")
        self._table_name = table_name
        self._column = watermark_column
        self._billing_project = billing_project
        self._client = client
        self._spark = spark

    def read(self) -> DataFrame:
        """Return a DataFrame over the rows the job needs."""
        spark = self._spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.active()
        defaults: dict[str, str | int | float | bool] = {"table": self._table_name}
        if self._billing_project is not None:
            defaults["parentProject"] = self._billing_project
        column = self._column
        if column is None:
            return self._load(spark, _merge(defaults, self._options))

        quoted = _quote(column)
        lower = f"{quoted} > {self._value}" if self._value is not None else None
        query = (
            f"SELECT IF(MAX({quoted}) IS NULL, NULL, FORMAT('%T', MAX({quoted}))) "
            f"AS upper_bound FROM {_quote_table(self._parts)}"
        )
        if lower is not None:
            query += f" WHERE {lower}"

        client = self._client or _new_client(self._billing_project)
        try:
            first = next(iter(client.query(query).result()), None)
        finally:
            if self._client is None:
                _close(client)
        # `first` is None when no row qualifies; the bound is its first field, by position.
        upper = None if first is None else first[0]

        if upper is None:
            defaults["filter"] = "FALSE"
        else:
            upper_predicate = f"{quoted} <= {upper}"
            defaults["filter"] = (
                upper_predicate if lower is None else f"{lower} AND {upper_predicate}"
            )
        return self._load(spark, _merge(defaults, self._options))

    @staticmethod
    def _load(spark: SparkSession, options: _Options) -> DataFrame:
        return spark.read.format("bigquery").options(**options).load()

    def __repr__(self) -> str:
        return f"BigQueryReader(table_name={self._table_name!r})"


class BigQueryWriter:
    """Writes a Spark DataFrame into a BigQuery table, atomically.

    Args:
        table_name: Table to write into, `project.dataset.table`. It must exist.
        billing_project: Project billed for the load and for the statements that apply it.
            The default project of the environment's credentials when omitted.
        options: Options of Spark's BigQuery data source passed through, such as
            `writeMethod` or `temporaryGcsBucket`. They override the connector's defaults.
        client: BigQuery client for the staging table and the statements. A new one is
            created at each write when omitted.
    """

    def __init__(
        self,
        table_name: str,
        *,
        billing_project: str | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        client: Client | None = None,
    ) -> None:
        self._parts = _split_table(table_name)
        _check_billing_project(billing_project)
        self._options = _check_options(options, _WRITER_RESERVED)
        _require_bigquery("BigQueryWriter")
        self._table_name = table_name
        self._billing_project = billing_project
        self._client = client

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` to the target, in one statement.

        Args:
            data: The rows to write.
            replace: Make the rows of `data` the target's only rows, in the same statement.
                Appends when `False`.
        """
        from google.cloud import bigquery

        columns = list(data.columns)
        if not columns:
            raise ValueError("the DataFrame has no columns")

        client = self._client or _new_client(self._billing_project)
        try:
            project, dataset, _ = self._parts
            staging_name = f"ingestio_staging_{uuid.uuid4().hex}"
            staging_id = f"{project}.{dataset}.{staging_name}"
            target = _quote_table(self._parts)
            staging = _quote_table((project, dataset, staging_name))
            cols = ", ".join(_quote(column) for column in columns)

            # Setup: a failure here leaves nothing behind, so nothing is cleaned up.
            existing = client.get_table(self._table_name)
            by_name = {field.name.lower(): field for field in existing.schema}
            schema: list[SchemaField] = []
            for column in columns:
                match = by_name.get(column.lower())
                if match is None:
                    raise ValueError(f"table '{self._table_name}' has no column '{column}'")
                schema.append(_staging_field(column, match))
            table = bigquery.Table(staging_id, schema=schema)
            table.expires = datetime.now(UTC) + timedelta(days=1)
            client.create_table(table)

            try:
                load_options = _merge(
                    {
                        "table": staging_id,
                        "writeMethod": "direct",
                        **(
                            {"parentProject": self._billing_project}
                            if self._billing_project is not None
                            else {}
                        ),
                    },
                    self._options,
                )
                data.write.format("bigquery").options(**load_options).mode("append").save()
                if replace:
                    source_cols = ", ".join(f"S.{_quote(column)}" for column in columns)
                    statement = (
                        f"MERGE {target} AS T USING {staging} AS S ON FALSE "
                        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({source_cols}) "
                        "WHEN NOT MATCHED BY SOURCE THEN DELETE"
                    )
                else:
                    statement = f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging}"
                client.query(statement).result()
            except BaseException as error:
                try:
                    client.delete_table(staging_id, not_found_ok=True)
                except Exception as cleanup_error:
                    error.add_note(
                        f"staging table {staging_id} could not be dropped: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
                raise
            # The rows are written; if this fails, the staging table expires on its own.
            with contextlib.suppress(Exception):
                client.delete_table(staging_id, not_found_ok=True)
        finally:
            if self._client is None:
                _close(client)

    def __repr__(self) -> str:
        return f"BigQueryWriter(table_name={self._table_name!r})"
