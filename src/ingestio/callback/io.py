"""Callbacks that move data between a connector and the shared state."""

from __future__ import annotations

from typing import Any

from ingestio.callback.core import Callback
from ingestio.connector.core import Reader, Writer

__all__ = ["ReadSource", "WriteTarget"]


class ReadSource(Callback):
    """Reads from a connector and stores the payload in the shared state.

    Args:
        reader: Connector to read from.
        into: Key of `Process.state` that receives the payload.
    """

    def __init__(self, reader: Reader[Any], *, into: str = "records") -> None:
        self.reader = reader
        self.into = into

    def run(self) -> None:
        self.process.state[self.into] = self.reader.read()


class WriteTarget(Callback):
    """Writes a value from the shared state through a connector.

    Args:
        writer: Connector to write to.
        key: Key of `Process.state` holding the value to write.
    """

    def __init__(self, writer: Writer[Any], *, key: str = "records") -> None:
        self.writer = writer
        self.key = key

    def run(self) -> None:
        self.writer.write(self.process.state[self.key])
