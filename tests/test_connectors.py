"""Connector behaviour: reading a public API and writing to a local volume."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.error import HTTPError

import pytest

from ingestio.connector.all import JsonApiReader, JsonLinesWriter


def test_reads_json_from_a_public_endpoint(api_url: str, payload: list[dict[str, object]]) -> None:
    assert JsonApiReader(api_url).read() == payload


def test_http_errors_are_not_swallowed(api_url: str) -> None:
    reader = JsonApiReader(api_url.replace("/records", "/boom"))
    with pytest.raises(HTTPError):
        reader.read()


@pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://example.com/data"])
def test_non_http_schemes_are_rejected_at_construction(url: str) -> None:
    with pytest.raises(ValueError, match="unsupported URL scheme"):
        JsonApiReader(url)


def test_writes_one_json_object_per_line(tmp_path: Path, payload: list[dict[str, object]]) -> None:
    target = tmp_path / "volume" / "orders.jsonl"
    JsonLinesWriter(target).write(payload)

    lines = target.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == payload


def test_missing_parent_directories_are_created(tmp_path: Path) -> None:
    target = tmp_path / "a" / "b" / "c" / "out.jsonl"
    JsonLinesWriter(target).write([{"id": 1}])
    assert target.exists()


def test_writing_twice_replaces_by_default(tmp_path: Path) -> None:
    target = tmp_path / "out.jsonl"
    writer = JsonLinesWriter(target)
    writer.write([{"id": 1}])
    writer.write([{"id": 2}])
    assert target.read_text(encoding="utf-8").splitlines() == ['{"id": 2}']


def test_append_mode_keeps_previous_records(tmp_path: Path) -> None:
    target = tmp_path / "out.jsonl"
    JsonLinesWriter(target).write([{"id": 1}])
    JsonLinesWriter(target, append=True).write([{"id": 2}])
    assert len(target.read_text(encoding="utf-8").splitlines()) == 2


def test_non_serialisable_values_do_not_break_the_write(tmp_path: Path) -> None:
    from datetime import UTC, datetime

    target = tmp_path / "out.jsonl"
    JsonLinesWriter(target).write([{"at": datetime(2026, 1, 1, tzinfo=UTC)}])
    assert "2026-01-01" in target.read_text(encoding="utf-8")
