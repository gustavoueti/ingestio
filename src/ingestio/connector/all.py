"""Import surface for the connector subpackage."""

from ingestio.connector.core import Reader, Writer
from ingestio.connector.http import JsonApiReader
from ingestio.connector.local import DiskReader, DiskWriter, JsonLinesWriter
from ingestio.connector.postgres import PostgresConnection, PostgresReader, PostgresWriter

__all__ = [
    "DiskReader",
    "DiskWriter",
    "JsonApiReader",
    "JsonLinesWriter",
    "PostgresConnection",
    "PostgresReader",
    "PostgresWriter",
    "Reader",
    "Writer",
]
