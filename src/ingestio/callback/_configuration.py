"""Reads a job's JSON configuration and builds the connector that one section names.

Private to the library. The table of systems is closed: it holds the connectors the library
ships, and nothing outside the library can add to it.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

from ingestio.connector.bigquery import BigQueryReader, BigQueryWriter
from ingestio.connector.gcs import GcsReader, GcsWriter
from ingestio.connector.http import JsonApiReader
from ingestio.connector.local import DiskReader, DiskWriter, JsonLinesWriter
from ingestio.connector.postgres import PostgresReader, PostgresWriter
from ingestio.connector.s3 import S3Reader, S3Writer
from ingestio.connector.unity_catalog import UnityCatalogReader, UnityCatalogWriter
from ingestio.connector.volume import VolumeReader, VolumeWriter

__all__ = ["build_connector"]

# The order in which the callback arguments are checked.
_ARGUMENTS = ("connection", "client", "watermark_value", "spark")


@dataclass(frozen=True)
class _System:
    """One connector of the table: its class, its keys and the callback arguments it takes."""

    connector: Callable[..., Any]
    required: tuple[str, ...]
    optional: tuple[str, ...] = ()
    arguments: tuple[str, ...] = ()
    # Arguments that must be given. Only `connection` is ever required.
    needed: tuple[str, ...] = ()


_FILE_READ = ("watermark_pattern", "schema", "options")
_SPARK_READ = ("watermark_value", "spark")

_SOURCES: Mapping[str, _System] = MappingProxyType(
    {
        "json_api": _System(JsonApiReader, ("url",), ("timeout", "headers", "encoding")),
        "disk": _System(DiskReader, ("path", "format"), _FILE_READ, _SPARK_READ),
        "postgres": _System(
            PostgresReader,
            ("table_name",),
            ("watermark_column", "options"),
            ("connection", *_SPARK_READ),
            ("connection",),
        ),
        "s3": _System(S3Reader, ("path", "format"), _FILE_READ, ("client", *_SPARK_READ)),
        "gcs": _System(GcsReader, ("path", "format"), _FILE_READ, ("client", *_SPARK_READ)),
        "volume": _System(
            VolumeReader, ("volume", "path", "format"), _FILE_READ, ("client", *_SPARK_READ)
        ),
        "unity_catalog": _System(
            UnityCatalogReader, ("table_name",), ("watermark_column", "options"), _SPARK_READ
        ),
        "bigquery": _System(
            BigQueryReader,
            ("table_name",),
            ("watermark_column", "billing_project", "options"),
            ("client", *_SPARK_READ),
        ),
    }
)

_TARGETS: Mapping[str, _System] = MappingProxyType(
    {
        "json_lines": _System(JsonLinesWriter, ("path",), ("encoding",)),
        "disk": _System(DiskWriter, ("path", "format"), ("options",)),
        "postgres": _System(
            PostgresWriter, ("table_name",), ("options",), ("connection",), ("connection",)
        ),
        "s3": _System(S3Writer, ("path", "format"), ("options",)),
        "gcs": _System(GcsWriter, ("path", "format"), ("options",)),
        "volume": _System(VolumeWriter, ("volume", "path", "format"), ("options",)),
        "unity_catalog": _System(UnityCatalogWriter, ("table_name",), ("options",)),
        "bigquery": _System(
            BigQueryWriter, ("table_name",), ("billing_project", "options"), ("client",)
        ),
    }
)

_SYSTEMS = MappingProxyType({"source": _SOURCES, "target": _TARGETS})

_STRING_KEYS = frozenset(
    {
        "system",
        "url",
        "encoding",
        "path",
        "format",
        "watermark_pattern",
        "schema",
        "table_name",
        "watermark_column",
        "volume",
        "billing_project",
    }
)


class _ConstantError(Exception):
    """`NaN`, `Infinity` or `-Infinity` in the text."""


def _reject_constant(name: str) -> object:
    raise _ConstantError(name)


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _type_error(key: str) -> str | None:
    """The end of the message for `key`, or `None` when `key` is a string key."""
    if key in _STRING_KEYS:
        return "a string"
    return {
        "timeout": "a number",
        "headers": "an object of strings",
        "options": "an object of strings, numbers and booleans",
    }[key]


def _well_typed(key: str, value: object) -> bool:
    if key in _STRING_KEYS:
        return isinstance(value, str)
    if key == "timeout":
        return _is_number(value)
    if key == "headers":
        return isinstance(value, dict) and all(isinstance(item, str) for item in value.values())
    return isinstance(value, dict) and all(
        isinstance(item, str | int | float | bool) for item in value.values()
    )


def build_connector(
    config: str | os.PathLike[str],
    section: str,
    arguments: Mapping[str, object | None],
) -> Any:
    """Read `config`, check its `section` and build the connector that `system` names.

    Args:
        config: Path of the JSON configuration file.
        section: `source` or `target`.
        arguments: The callback arguments, by name, `None` for the ones not given.

    Raises:
        TypeError: `config` is not a path.
        ValueError: The file or the callback arguments do not describe a connector.
    """
    if not isinstance(config, str | os.PathLike):
        raise TypeError(f"config must be a str or os.PathLike, not {type(config).__name__}")
    path = os.fspath(config)

    def fail(message: str) -> ValueError:
        return ValueError(f"{path}: {message}")

    text = Path(config).read_text(encoding="utf-8")
    try:
        document = json.loads(text, parse_constant=_reject_constant)
    except _ConstantError as exc:
        raise fail(f"invalid JSON: {exc.args[0]} is not allowed") from None
    except json.JSONDecodeError as exc:
        raise fail(f"invalid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise fail("the configuration must be a JSON object")
    if section not in document:
        raise fail(f"missing section '{section}'")
    keys = document[section]
    if not isinstance(keys, dict):
        raise fail(f"section '{section}' must be a JSON object")

    systems = _SYSTEMS[section]
    if "system" not in keys:
        raise fail(f"{section}.system is required")
    name = keys["system"]
    if not isinstance(name, str):
        raise fail(f"{section}.system must be a string")
    if name not in systems:
        names = ", ".join(sorted(systems))
        raise fail(f"{section}.system '{name}' is not one of: {names}")
    system = systems[name]

    allowed = (*system.required, *system.optional)
    for key in keys:
        if key != "system" and key not in allowed:
            raise fail(f"{section}.{key} is not a key of system '{name}'")
    for key in system.required:
        if key not in keys:
            raise fail(f"{section}.{key} is required for system '{name}'")
    for key, value in keys.items():
        if not _well_typed(key, value):
            raise fail(f"{section}.{key} must be {_type_error(key)}")

    taken: dict[str, object] = {}
    for argument in _ARGUMENTS:
        if argument not in arguments or arguments[argument] is None:
            continue
        if argument not in system.arguments:
            raise fail(f"{section} system '{name}' takes no argument {argument}")
        taken[argument] = arguments[argument]
    for argument in system.needed:
        if argument not in taken:
            raise fail(f"{section} system '{name}' needs the argument {argument}")

    parameters = {key: value for key, value in keys.items() if key != "system"}
    parameters.update(taken)
    try:
        return system.connector(**parameters)
    except Exception as exc:
        exc.add_note(f"while building the {section} of {path}, system '{name}'")
        raise
