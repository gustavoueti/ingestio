"""Import surface for the connector subpackage."""

from ingestio.connector.core import Reader, Writer
from ingestio.connector.http import JsonApiReader
from ingestio.connector.local import JsonLinesWriter

__all__ = ["JsonApiReader", "JsonLinesWriter", "Reader", "Writer"]
