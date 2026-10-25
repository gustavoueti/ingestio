"""Callbacks that move data between the job's configured systems and the shared state."""

from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from functools import reduce
from typing import TYPE_CHECKING

from ingestio.callback._configuration import Scd2Settings, build_connector, build_scd2
from ingestio.callback.core import Callback

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

    from ingestio.connector.mongodb import MongoDBConnection
    from ingestio.connector.mysql import MySQLConnection
    from ingestio.connector.postgres import PostgresConnection

__all__ = ["SCD2", "AppendOnly", "FullLoad", "ReadSource"]


class ReadSource(Callback):
    """Reads from the source that the job's configuration names, into the shared state.

    Args:
        config: Path of the job's JSON configuration file. Its `source` section names the
            system and holds the reader's parameters.
        into: Key of `Process.state` that receives the payload.
        connection: Where a PostgreSQL, MongoDB or MySQL source lives, with its credentials: a
            `PostgresConnection` for the `postgres` system, a `MongoDBConnection` for the
            `mongodb` system, a `MySQLConnection` for the `mysql` system. Required by all three.
        client: SDK client of the source, for the systems that take one.
        watermark_value: Last watermark already ingested, for the systems that take one.
        spark: Session to read with, for the Spark systems. The active session when omitted.
    """

    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        into: str = "records",
        connection: PostgresConnection | MongoDBConnection | MySQLConnection | None = None,
        client: object | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._reader = build_connector(
            config,
            "source",
            {
                "connection": connection,
                "client": client,
                "watermark_value": watermark_value,
                "spark": spark,
            },
        )
        self.into = into

    def run(self) -> None:
        self.process.state[self.into] = self._reader.read()


class AppendOnly(Callback):
    """Appends a value of the shared state to the target that the job's configuration names.

    What the target already holds stays as it is.

    Args:
        config: Path of the job's JSON configuration file. Its `target` section names the
            system and holds the writer's parameters.
        key: Key of `Process.state` holding the value to write.
        connection: Where a PostgreSQL or MySQL target lives, with its credentials: a
            `PostgresConnection` for the `postgres` system, a `MySQLConnection` for the `mysql`
            system. Required by both.
        client: SDK client of the target, for the systems that take one.
    """

    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        key: str = "records",
        connection: PostgresConnection | MySQLConnection | None = None,
        client: object | None = None,
    ) -> None:
        self._writer = build_connector(
            config, "target", {"connection": connection, "client": client}
        )
        self.key = key

    def run(self) -> None:
        self._writer.write(self.process.state[self.key])


class FullLoad(Callback):
    """Makes the target that the job's configuration names hold exactly a value of the state.

    Args:
        config: Path of the job's JSON configuration file. Its `target` section names the
            system and holds the writer's parameters.
        key: Key of `Process.state` holding the value to write.
        connection: Where a PostgreSQL or MySQL target lives, with its credentials: a
            `PostgresConnection` for the `postgres` system, a `MySQLConnection` for the `mysql`
            system. Required by both.
        client: SDK client of the target, for the systems that take one.
    """

    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        key: str = "records",
        connection: PostgresConnection | MySQLConnection | None = None,
        client: object | None = None,
    ) -> None:
        self._writer = build_connector(
            config, "target", {"connection": connection, "client": client}
        )
        self.key = key

    def run(self) -> None:
        self._writer.write(self.process.state[self.key], replace=True)


# Names of the helper columns of the chain.
_KIND = "__ingestio_kind"
_TIME = "__ingestio_time"
_ROW = "__ingestio_row"
_PREV_KIND = "__ingestio_prev_kind"
_PREV_TIME = "__ingestio_prev_time"
_PREV_VALUE = "__ingestio_prev_value_"
_NEW = "__ingestio_new"
_CLOSED = "__ingestio_closed"
_NEXT = "__ingestio_next"
_CURRENT = "__ingestio_current"
_CODE = "__ingestio_code"
_ENTITY_CODE = "__ingestio_entity_code"


class SCD2(Callback):
    """Keeps the history of the dimension that the job's configuration names, as an SCD of type 2.

    Args:
        config: Path of the job's JSON configuration file. Its `target` section names the
            dimension's system and table, and its `scd2` section holds the key columns, the
            effective-time column and the other parameters of the history.
        key: Key of `Process.state` holding the batch, a Spark DataFrame.
        connection: Where a PostgreSQL or MySQL target lives, with its credentials: a
            `PostgresConnection` for the `postgres` system, a `MySQLConnection` for the `mysql`
            system. Required by both.
        client: BigQuery client of the target, for the `bigquery` system.
    """

    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        key: str = "records",
        connection: PostgresConnection | MySQLConnection | None = None,
        client: object | None = None,
    ) -> None:
        plan = build_scd2(config, {"connection": connection, "client": client})
        self._writer = plan.writer
        self._reader = plan.reader
        self._settings = plan.settings
        self.key = key

    def run(self) -> None:
        key = self.key
        settings = self._settings
        batch = self.process.state[key]

        # Steps 2 and 4 use only the names of the columns: they run no Spark job.
        batch_columns: list[str] = list(batch.columns)
        in_batch = {name.lower(): name for name in batch_columns}
        named = (
            [("key_columns", name) for name in settings.key_columns]
            + [("effective_time_column", settings.effective_time_column)]
            + [("tracked_columns", name) for name in settings.tracked_columns or ()]
        )
        for parameter, name in named:
            if name.lower() not in in_batch:
                raise ValueError(
                    f"state['{key}'] has no column '{name}', which scd2.{parameter} names"
                )
        for parameter, name in (
            ("valid_from_column", settings.valid_from_column),
            ("valid_to_column", settings.valid_to_column),
        ):
            if name.lower() in in_batch:
                raise ValueError(
                    f"state['{key}'] already has a column '{name}', which scd2.{parameter} names"
                )

        target = self._reader.build(batch.sparkSession).read()

        target_columns: list[str] = list(target.columns)
        in_target = {name.lower(): name for name in target_columns}
        expected = [*batch_columns, settings.valid_from_column, settings.valid_to_column]
        for name in expected:
            if name.lower() not in in_target:
                raise ValueError(f"the target has no column '{name}'")
        known = {name.lower() for name in expected}
        for name in target_columns:
            if name.lower() not in known:
                raise ValueError(
                    f"the target has a column '{name}' that is neither a column of "
                    f"state['{key}'] nor a version column"
                )

        new_table = _new_table(key, settings, batch, target, in_batch, in_target)
        self._writer.write(new_table, replace=True)


def _quoted(name: str) -> Column:
    from pyspark.sql import functions

    return functions.col("`" + name.replace("`", "``") + "`")


def _new_table(
    key: str,
    settings: Scd2Settings,
    batch: DataFrame,
    target: DataFrame,
    in_batch: dict[str, str],
    in_target: dict[str, str],
) -> DataFrame:
    """Check the batch against the target and return the new table, guarded.

    Raises:
        ValueError: One of the four checks fails.
    """
    from pyspark.sql import Window, functions

    def of_target(name: str) -> str:
        return in_target[name.lower()]

    keys = [of_target(name) for name in settings.key_columns]
    valid_from = of_target(settings.valid_from_column)
    valid_to = of_target(settings.valid_to_column)
    batch_time = in_batch[settings.effective_time_column.lower()]
    if settings.tracked_columns is None:
        excluded = {name.lower() for name in (*settings.key_columns, batch_time)}
        tracked = [of_target(name) for name in in_batch.values() if name.lower() not in excluded]
    else:
        tracked = [of_target(name) for name in settings.tracked_columns]
    columns = list(target.columns)
    schema = target.schema
    from_type = schema[valid_from].dataType
    to_type = schema[valid_to].dataType

    # The chain: the current version of each entity, then the records of the batch. The closed
    # versions of the target travel in the same scan, kind 2, in partitions of their own, so
    # that the plan reads the target once. They stay out of the chain and come out as read.
    current = target.select(
        *[_quoted(name) for name in columns],
        functions.when(_quoted(valid_to).isNull(), functions.lit(0))
        .otherwise(functions.lit(2))
        .alias(_KIND),
        _quoted(valid_from).alias(_TIME),
    )

    def record_column(name: str) -> Column:
        if name == valid_from:
            return _quoted(batch_time).alias(name)
        if name == valid_to:
            return functions.lit(None).cast(to_type).alias(name)
        return _quoted(in_batch[name.lower()]).alias(name)

    records = batch.select(
        *[record_column(name) for name in columns],
        functions.lit(1).alias(_KIND),
        _quoted(batch_time).alias(_TIME),
    )
    chain = current.unionByName(records)
    chain = chain.select("*", (functions.col(_KIND) == 2).alias(_CLOSED))

    entity = [*[_quoted(name) for name in keys], functions.col(_CLOSED)]
    in_order = Window.partitionBy(*entity).orderBy(
        functions.col(_TIME).asc(), functions.col(_KIND).asc()
    )
    after = in_order.rowsBetween(1, Window.unboundedFollowing)
    in_entity = Window.partitionBy(*entity)

    chain = chain.select(
        "*",
        functions.row_number().over(in_order).alias(_ROW),
        functions.lag(_KIND).over(in_order).alias(_PREV_KIND),
        functions.lag(_TIME).over(in_order).alias(_PREV_TIME),
        *[
            functions.lag(_quoted(name)).over(in_order).alias(f"{_PREV_VALUE}{i}")
            for i, name in enumerate(tracked)
        ],
    )
    same: Column = reduce(
        lambda left, right: left & right,
        [
            _quoted(name).eqNullSafe(functions.col(f"{_PREV_VALUE}{i}"))
            for i, name in enumerate(tracked)
        ],
        functions.lit(True),
    )
    is_record = functions.col(_KIND) == 1
    chain = chain.select("*", (is_record & ((functions.col(_ROW) == 1) | ~same)).alias(_NEW))
    chain = chain.select(
        "*",
        functions.min(functions.when(functions.col(_NEW), functions.col(_TIME)))
        .over(after)
        .alias(_NEXT),
        functions.min(functions.when(functions.col(_KIND) == 0, functions.col(_TIME)))
        .over(in_entity)
        .alias(_CURRENT),
    )

    # The checks, in the order of the spec; the first condition that holds gives the code.
    messages = [f"state['{key}'] has a null in column '{name}'" for name in settings.key_columns]
    messages.append(f"state['{key}'] has a null in column '{settings.effective_time_column}'")
    messages.append(
        f"state['{key}'] has records with the same key columns and the same "
        f"'{settings.effective_time_column}'"
    )
    messages.append(
        f"state['{key}'] has a record whose '{settings.effective_time_column}' is earlier than "
        f"the '{settings.valid_from_column}' of its entity's current version"
    )
    messages.append(
        f"state['{key}'] has a record whose '{settings.effective_time_column}' equals the "
        f"'{settings.valid_from_column}' of its entity's current version, with other tracked values"
    )
    same_time = functions.col(_TIME) == functions.col(_PREV_TIME)
    conditions: list[Column] = [is_record & _quoted(name).isNull() for name in keys]
    conditions += [
        is_record & functions.col(_TIME).isNull(),
        is_record & (functions.col(_PREV_KIND) == 1) & same_time,
        is_record & (functions.col(_TIME) < functions.col(_CURRENT)),
        is_record & (functions.col(_PREV_KIND) == 0) & same_time & ~same,
    ]
    code = functions.when(conditions[0], functions.lit(0))
    for index, condition in enumerate(conditions[1:], start=1):
        code = code.when(condition, functions.lit(index))
    chain = chain.select("*", code.alias(_CODE))

    row = chain.agg(functions.min(_CODE)).first()
    failed = None if row is None else row[0]
    if failed is not None:
        raise ValueError(messages[failed])

    # The guard: the write evaluates the plan again, and a rejected record fails that evaluation.
    chain = chain.select("*", functions.min(_CODE).over(in_entity).alias(_ENTITY_CODE))
    guarded: Column | None = None
    for index, message in enumerate(messages):
        branch = functions.raise_error(message)
        guarded = (
            functions.when(functions.col(_ENTITY_CODE) == index, branch)
            if guarded is None
            else guarded.when(functions.col(_ENTITY_CODE) == index, branch)
        )
    assert guarded is not None
    guarded = guarded.otherwise(functions.col(_TIME)).cast(from_type)

    def output(name: str) -> Column:
        if name == valid_from:
            return (
                functions.when(functions.col(_CLOSED), _quoted(name).cast(from_type))
                .otherwise(guarded)
                .alias(name)
            )
        if name == valid_to:
            return (
                functions.when(functions.col(_CLOSED), _quoted(name).cast(to_type))
                .otherwise(functions.col(_NEXT).cast(to_type))
                .alias(name)
            )
        return _quoted(name)

    kept = chain.filter((functions.col(_KIND) != 1) | functions.col(_NEW))
    return kept.select(*[output(name) for name in columns])
