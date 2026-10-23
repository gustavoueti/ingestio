"""`log_to_console`: show the progress tree of a run on a stream, in color on a terminal.

The engine writes every line of the tree as plain text and attaches a `TreeLine` to the record
through `extra`. The handler here colors from that data and never parses the message, because
step names, callback names and exception messages are arbitrary text. `TreeLine` and the
handler are internal; only `log_to_console` is public.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from typing import TextIO

__all__ = ["log_to_console"]

#: Key of the `extra` entry that carries a `TreeLine` on the records of the tree.
RECORD_KEY = "ingestio_tree_line"

_RESET = "\x1b[0m"
_DIM = "\x1b[2m"
_COLORS = {
    "start": "\x1b[36m",
    "ok": "\x1b[32m",
    "fail": "\x1b[31m",
    "cancel": "\x1b[33m",
}
# Only these outcomes color the text after the marker.
_COLORED_TEXT = frozenset({"fail", "cancel"})


@dataclass(frozen=True)
class TreeLine:
    """One event of the tree: what the engine writes and what the handler needs to color it.

    Args:
        prefix: `[<run id>] ` followed by the guides.
        marker: The one character that opens the line.
        text: Text after the marker. May span several lines.
        outcome: `start`, `ok`, `fail` or `cancel`.
    """

    prefix: str
    marker: str
    text: str
    outcome: str

    def _text_lines(self) -> list[str]:
        return self.text.splitlines() or [""]

    def plain(self) -> str:
        """The line as the message of the record, one row per line of the text."""
        first, *rest = self._text_lines()
        rows = [f"{self.prefix}{self.marker} {first}"]
        rows.extend(f"{self.prefix}  {row}" for row in rest)
        return "\n".join(rows)

    def colored(self) -> str:
        """The same rows with SGR sequences, each colored part closed by a reset."""
        color = _COLORS[self.outcome]
        text_color = color if self.outcome in _COLORED_TEXT else ""

        def text(row: str) -> str:
            return f"{text_color}{row}{_RESET}" if text_color else row

        dim_prefix = f"{_DIM}{self.prefix}{_RESET}"
        first, *rest = self._text_lines()
        rows = [f"{dim_prefix}{color}{self.marker}{_RESET} {text(first)}"]
        rows.extend(f"{dim_prefix}  {text(row)}" for row in rest)
        return "\n".join(rows)


class _ConsoleFormatter(logging.Formatter):
    """Writes the tree as its message, or colored; any other record as its message alone.

    Args:
        color: Whether to add SGR sequences to the lines of the tree.
    """

    def __init__(self, *, color: bool) -> None:
        super().__init__("%(message)s")
        self._color = color

    def format(self, record: logging.LogRecord) -> str:
        line = getattr(record, RECORD_KEY, None)
        if not isinstance(line, TreeLine):
            return super().format(record)
        return line.colored() if self._color else line.plain()


# The handler attached by the last call, so that the next call can replace it.
_handler: logging.Handler | None = None


def _wants_color(stream: TextIO, color: bool | None) -> bool:
    if color is not None:
        return color
    # no-color.org: a non-empty NO_COLOR turns color off.
    if os.environ.get("NO_COLOR"):
        return False
    isatty = getattr(stream, "isatty", None)
    return bool(isatty()) if callable(isatty) else False


def log_to_console(
    *,
    stream: TextIO | None = None,
    level: int = logging.INFO,
    color: bool | None = None,
) -> logging.Handler:
    """Show the progress tree of every run on a stream.

    Args:
        stream: Stream to write to. `sys.stderr`, as it is when the function is called,
            when omitted.
        level: Lowest level written.
        color: `True` always colors, `False` never does, `None` colors when the stream is a
            terminal and `NO_COLOR` is unset or empty.

    Returns:
        The handler attached to the `ingestio` logger.

    Raises:
        ValueError: If `level` is not above `logging.NOTSET`.
    """
    # A logger at NOTSET inherits its level, so lowering it there would hide the lines.
    if level <= logging.NOTSET:
        raise ValueError("level must be above NOTSET")
    global _handler
    target = sys.stderr if stream is None else stream
    package_logger = logging.getLogger("ingestio")

    if _handler is not None:
        package_logger.removeHandler(_handler)

    handler = logging.StreamHandler(target)
    handler.setLevel(level)
    handler.setFormatter(_ConsoleFormatter(color=_wants_color(target, color)))
    package_logger.addHandler(handler)
    _handler = handler

    # Lower the level so the records reach the handler, but never raise it.
    if package_logger.getEffectiveLevel() > level:
        package_logger.setLevel(level)
    return handler
