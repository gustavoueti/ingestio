"""Small, composable library for building data ingestion pipelines."""

import logging

from ingestio.basics import (
    SCD2,
    AppendOnly,
    Callback,
    CancelProcessException,
    CancelStepException,
    FullLoad,
    Process,
    Reader,
    ReadSource,
    Step,
    Writer,
    log_to_console,
)

__version__ = "0.5.0"

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "SCD2",
    "AppendOnly",
    "Callback",
    "CancelProcessException",
    "CancelStepException",
    "FullLoad",
    "Process",
    "ReadSource",
    "Reader",
    "Step",
    "Writer",
    "__version__",
    "log_to_console",
]
