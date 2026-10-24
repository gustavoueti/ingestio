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

__all__ = ["Scd2Plan", "Scd2Settings", "TargetReader", "build_connector", "build_scd2"]

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


def _load(config: str | os.PathLike[str]) -> tuple[str, dict[str, Any]]:
    """Read `config` and return its path as text and its top-level JSON object."""
    if not isinstance(config, str | os.PathLike):
        raise TypeError(f"config must be a str or os.PathLike, not {type(config).__name__}")
    path = os.fspath(config)
    text = Path(config).read_text(encoding="utf-8")
    try:
        document = json.loads(text, parse_constant=_reject_constant)
    except _ConstantError as exc:
        raise _fail(path, f"invalid JSON: {exc.args[0]} is not allowed") from None
    except json.JSONDecodeError as exc:
        raise _fail(path, f"invalid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise _fail(path, "the configuration must be a JSON object")
    return path, document


def _fail(path: str, message: str) -> ValueError:
    return ValueError(f"{path}: {message}")


def _section(path: str, document: dict[str, Any], section: str) -> dict[str, Any]:
    if section not in document:
        raise _fail(path, f"missing section '{section}'")
    keys = document[section]
    if not isinstance(keys, dict):
        raise _fail(path, f"section '{section}' must be a JSON object")
    return keys


def _build(
    path: str,
    document: dict[str, Any],
    section: str,
    arguments: Mapping[str, object | None],
    systems: Mapping[str, _System],
) -> tuple[Any, str, dict[str, Any], dict[str, object]]:
    """Check `section` and build its connector.

    Returns the connector, the name of the system, the keys of the section and the callback
    arguments that the system took.
    """
    keys = _section(path, document, section)
    if "system" not in keys:
        raise _fail(path, f"{section}.system is required")
    name = keys["system"]
    if not isinstance(name, str):
        raise _fail(path, f"{section}.system must be a string")
    if name not in systems:
        names = ", ".join(sorted(systems))
        raise _fail(path, f"{section}.system '{name}' is not one of: {names}")
    system = systems[name]

    allowed = (*system.required, *system.optional)
    for key in keys:
        if key != "system" and key not in allowed:
            raise _fail(path, f"{section}.{key} is not a key of system '{name}'")
    for key in system.required:
        if key not in keys:
            raise _fail(path, f"{section}.{key} is required for system '{name}'")
    for key, value in keys.items():
        if not _well_typed(key, value):
            raise _fail(path, f"{section}.{key} must be {_type_error(key)}")

    taken: dict[str, object] = {}
    for argument in _ARGUMENTS:
        if argument not in arguments or arguments[argument] is None:
            continue
        if argument not in system.arguments:
            raise _fail(path, f"{section} system '{name}' takes no argument {argument}")
        taken[argument] = arguments[argument]
    for argument in system.needed:
        if argument not in taken:
            raise _fail(path, f"{section} system '{name}' needs the argument {argument}")

    parameters = {key: value for key, value in keys.items() if key != "system"}
    parameters.update(taken)
    try:
        connector = system.connector(**parameters)
    except Exception as exc:
        exc.add_note(f"while building the {section} of {path}, system '{name}'")
        raise
    return connector, name, keys, taken


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
    path, document = _load(config)
    return _build(path, document, section, arguments, _SYSTEMS[section])[0]


# --- The scd2 section ---------------------------------------------------------------------

# The target systems that SCD2 supports.
_SCD2_SYSTEMS = MappingProxyType(
    {name: _TARGETS[name] for name in ("bigquery", "postgres", "unity_catalog")}
)
# The keys of the `target` section that the reader of the same system takes as well.
_READER_KEYS = ("table_name", "billing_project")

_SCD2_KEYS = (
    "key_columns",
    "effective_time_column",
    "tracked_columns",
    "valid_from_column",
    "valid_to_column",
)
_SCD2_REQUIRED = ("key_columns", "effective_time_column")
_SCD2_ARRAYS = ("key_columns", "tracked_columns")


@dataclass(frozen=True)
class Scd2Settings:
    """The checked `scd2` section, the names as the file writes them."""

    key_columns: tuple[str, ...]
    effective_time_column: str
    tracked_columns: tuple[str, ...] | None
    valid_from_column: str
    valid_to_column: str


@dataclass(frozen=True)
class TargetReader:
    """The reader of the target's system: its class and its parameters, but for the session."""

    connector: Callable[..., Any]
    parameters: Mapping[str, object]

    def build(self, spark: object) -> Any:
        return self.connector(**self.parameters, spark=spark)


@dataclass(frozen=True)
class Scd2Plan:
    """What `SCD2` needs from the file: the writer, the target's reader and the section."""

    writer: Any
    reader: TargetReader
    settings: Scd2Settings


def _scd2_settings(path: str, document: dict[str, Any]) -> Scd2Settings:
    section = "scd2"
    keys = _section(path, document, section)
    for key in keys:
        if key not in _SCD2_KEYS:
            raise _fail(path, f"{section}.{key} is not a key of section '{section}'")
    for key in _SCD2_REQUIRED:
        if key not in keys:
            raise _fail(path, f"{section}.{key} is required")
    for key, value in keys.items():
        if key in _SCD2_ARRAYS:
            if not (
                isinstance(value, list)
                and value
                and all(isinstance(item, str) and item for item in value)
            ):
                raise _fail(path, f"{section}.{key} must be a non-empty array of non-empty strings")
            seen: set[str] = set()
            for item in value:
                if item.lower() in seen:
                    raise _fail(path, f"{section}.{key} names the column '{item}' twice")
                seen.add(item.lower())
        elif not (isinstance(value, str) and value):
            raise _fail(path, f"{section}.{key} must be a non-empty string")

    valid_from = keys.get("valid_from_column", "valid_from")
    valid_to = keys.get("valid_to_column", "valid_to")
    roles: list[tuple[str, list[str]]] = [
        ("key_columns", keys["key_columns"]),
        ("effective_time_column", [keys["effective_time_column"]]),
    ]
    if "tracked_columns" in keys:
        roles.append(("tracked_columns", keys["tracked_columns"]))
    roles.append(("valid_from_column", [valid_from]))
    roles.append(("valid_to_column", [valid_to]))
    for index, (role, names) in enumerate(roles):
        for later, later_names in roles[index + 1 :]:
            lowered = {item.lower() for item in later_names}
            for item in names:
                if item.lower() in lowered:
                    raise _fail(
                        path,
                        f"{section}.{role} and {section}.{later} both name the column '{item}'",
                    )

    tracked = keys.get("tracked_columns")
    return Scd2Settings(
        key_columns=tuple(keys["key_columns"]),
        effective_time_column=keys["effective_time_column"],
        tracked_columns=None if tracked is None else tuple(tracked),
        valid_from_column=valid_from,
        valid_to_column=valid_to,
    )


def build_scd2(config: str | os.PathLike[str], arguments: Mapping[str, object | None]) -> Scd2Plan:
    """Read `config` once: build the writer of its `target` and check its `scd2` section.

    Args:
        config: Path of the JSON configuration file.
        arguments: The callback arguments `connection` and `client`, `None` for the ones not
            given.

    Raises:
        TypeError: `config` is not a path.
        ValueError: The file or the callback arguments do not describe a writer, or the `scd2`
            section is wrong.
    """
    path, document = _load(config)
    writer, name, keys, taken = _build(path, document, "target", arguments, _SCD2_SYSTEMS)
    settings = _scd2_settings(path, document)
    parameters: dict[str, object] = {
        key: value for key, value in keys.items() if key in _READER_KEYS
    }
    parameters.update(taken)
    return Scd2Plan(writer, TargetReader(_SOURCES[name].connector, parameters), settings)
