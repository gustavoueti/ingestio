"""Import surface for the connector subpackage."""

from ingestio.connector.core import Reader, Writer
from ingestio.connector.http import JsonApiReader
from ingestio.connector.local import JsonLinesWriter
from ingestio.connector.postgres import PostgresConnection, PostgresReader, PostgresWriter

__all__ = [
    "JsonApiReader",
    "JsonLinesWriter",
    "PostgresConnection",
    "PostgresReader",
    "PostgresWriter",
    "Reader",
    "Writer",
]
