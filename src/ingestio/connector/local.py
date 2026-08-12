"""Writer for local volumes.

A Databricks volume is reachable as an ordinary path, so the same writer serves a local
disk and a cluster.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

__all__ = ["JsonLinesWriter"]


class JsonLinesWriter:
    """Writes records as newline-delimited JSON, one object per line.

    Args:
        path: Destination file. Parent directories are created when missing.
        append: Append to the file instead of replacing it.
        encoding: Text encoding of the output file.
    """

    def __init__(
        self,
        path: Path | str,
        *,
        append: bool = False,
        encoding: str = "utf-8",
    ) -> None:
        self.path = Path(path)
        self.append = append
        self.encoding = encoding

    def write(self, data: Iterable[Mapping[str, Any]]) -> None:
        """Write every record as one JSON line."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if self.append else "w"
        with self.path.open(mode, encoding=self.encoding) as handle:
            for record in data:
                # default=str keeps datetimes and similar values writable.
                handle.write(json.dumps(record, ensure_ascii=False, default=str))
                handle.write("\n")

    def __repr__(self) -> str:
        return f"{type(self).__name__}(path={str(self.path)!r})"
