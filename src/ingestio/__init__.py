"""Small, composable library for building data ingestion pipelines."""

import logging

from ingestio.basics import (
    Callback,
    CancelProcessException,
    CancelStepException,
    Process,
    Reader,
    ReadSource,
    Step,
    Writer,
    WriteTarget,
)

__version__ = "0.0.1"

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "Callback",
    "CancelProcessException",
    "CancelStepException",
    "Process",
    "ReadSource",
    "Reader",
    "Step",
    "WriteTarget",
    "Writer",
    "__version__",
]
