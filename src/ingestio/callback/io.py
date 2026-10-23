"""Callbacks that move data between the job's configured systems and the shared state."""

from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

from ingestio.callback._configuration import build_connector
from ingestio.callback.core import Callback
from ingestio.connector.local import JsonLinesWriter

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ingestio.connector.postgres import PostgresConnection

__all__ = ["ReadSource", "WriteTarget"]


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
    ) -> None:
        self._writer = build_connector(
            config,
            "target",
            {"connection": connection, "client": client},
            append=not replace,
        )
        self.key = key
        self._replace = replace

    def run(self) -> None:
        data = self.process.state[self.key]
        # JsonLinesWriter chooses append or replace when it is built and has no `replace`.
        if self._replace and not isinstance(self._writer, JsonLinesWriter):
            self._writer.write(data, replace=True)
        else:
            self._writer.write(data)
