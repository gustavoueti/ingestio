"""Rules that the file connectors share: formats, options and the watermark selection.

Private to the package. A reader lists its source as (name, modification time) entries and
hands them to a `Watermark`; the rest is the same for every store.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypeAlias

__all__ = [
    "RESERVED_BOTH",
    "RESERVED_READER",
    "RESERVED_WRITER",
    "Options",
    "Watermark",
    "check_format",
    "check_options",
    "check_readable_format",
    "is_hidden",
    "units_since_epoch",
]

Options: TypeAlias = Mapping[str, str | int | float | bool]

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)

RESERVED_BOTH = ("path", "paths")
RESERVED_READER = ("pathglobfilter", "recursivefilelookup", "modifiedbefore", "modifiedafter")
RESERVED_WRITER = ("replacewhere", "partitionoverwritemode")


def check_format(format: str) -> None:  # noqa: A002 - the configuration key
    if not format:
        raise ValueError("format must not be empty")


def check_readable_format(format: str) -> None:  # noqa: A002 - the configuration key
    check_format(format)
    if format.lower() == "delta":
        raise ValueError(f"format '{format}' cannot be read file by file")


def check_options(options: Options | None, reserved: tuple[str, ...]) -> dict[str, Any]:
    given = dict(options or {})
    for key in given:
        if key.lower() in reserved:
            raise ValueError(f"option '{key}' is set by the connector")
    return given


def is_hidden(name: str) -> bool:
    """Spark's rule for hidden and in-progress files."""
    return name.startswith(("_", ".")) or name.endswith("._COPYING_")


def units_since_epoch(moment: datetime, unit_micros: int) -> int:
    """Whole `unit_micros` since the epoch, exactly and truncated."""
    return (moment - EPOCH) // _MICROSECOND // unit_micros


@dataclass(frozen=True)
class Watermark:
    """Where a file's watermark comes from and the value above which files are read.

    `unit_micros` is the precision of modification times, in microseconds. The entries given
    to `select` carry their time as whole units since the epoch.
    """

    pattern: re.Pattern[str] | None
    text: str | None
    instant: int | None

    @classmethod
    def parse(
        cls,
        pattern: str | None,
        value: str | datetime | None,
        unit_micros: int,
    ) -> Watermark:
        compiled: re.Pattern[str] | None = None
        if pattern is not None:
            try:
                compiled = re.compile(pattern)
            except re.error as exc:
                raise ValueError(f"invalid watermark_pattern: {exc}") from exc
            if compiled.groups != 1:
                raise ValueError("watermark_pattern must have exactly one group")
        text: str | None = None
        instant: int | None = None
        if value is not None:
            if pattern is not None:
                if not isinstance(value, str):
                    raise ValueError("watermark_value must be a str with watermark_pattern")
                text = value
            else:
                if not isinstance(value, datetime):
                    raise ValueError("watermark_value must be a datetime without watermark_pattern")
                if value.tzinfo is None or value.utcoffset() is None:
                    raise ValueError("watermark_value must be timezone-aware")
                instant = units_since_epoch(value, unit_micros)
        return cls(compiled, text, instant)

    @property
    def needs_time(self) -> bool:
        """Whether `select` looks at the modification times."""
        return self.instant is not None

    def select(self, entries: Iterable[tuple[str, int]]) -> list[str]:
        """Names above the watermark, sorted. Hidden names are the caller's to leave out."""
        selected: list[str] = []
        for name, time in entries:
            if self.pattern is not None:
                match = self.pattern.fullmatch(name)
                if match is None:
                    continue
                mark = match.group(1)
                if mark is None:
                    continue
                if self.text is not None and not mark > self.text:
                    continue
            elif self.instant is not None:
                if time <= self.instant:
                    continue
            selected.append(name)
        return sorted(selected)
