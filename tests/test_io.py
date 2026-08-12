"""`ReadSource` and `WriteTarget`, and one end-to-end ingestion from API to volume."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ingestio import Process, ReadSource, Step, WriteTarget
from ingestio.connector.all import JsonApiReader, JsonLinesWriter


class _StubReader:
    def __init__(self, value: Any) -> None:
        self.value = value

    def read(self) -> Any:
        return self.value


class _StubWriter:
    def __init__(self) -> None:
        self.written: list[Any] = []

    def write(self, data: Any) -> None:
        self.written.append(data)


def test_read_source_stores_the_payload_in_the_state() -> None:
    process = Process([Step("extract", [ReadSource(_StubReader([{"id": 1}]))])]).run()
    assert process.state["records"] == [{"id": 1}]


def test_write_target_writes_what_the_state_holds() -> None:
    writer = _StubWriter()
    Process(
        [Step("load", [WriteTarget(writer)])],
        state={"records": [{"id": 1}]},
    ).run()
    assert writer.written == [[{"id": 1}]]


def test_state_keys_are_configurable() -> None:
    writer = _StubWriter()
    Process(
        [
            Step("extract", [ReadSource(_StubReader("payload"), into="raw")]),
            Step("load", [WriteTarget(writer, key="raw")]),
        ]
    ).run()
    assert writer.written == ["payload"]


def test_ingests_from_api_into_a_volume(
    api_url: str, tmp_path: Path, payload: list[dict[str, Any]]
) -> None:
    target = tmp_path / "Volumes" / "bronze" / "orders.jsonl"

    process = Process(
        [
            Step("extract", [ReadSource(JsonApiReader(api_url))]),
            Step("load", [WriteTarget(JsonLinesWriter(target))]),
        ],
        run_id="run0001",
    ).run()

    written = [json.loads(line) for line in target.read_text("utf-8").splitlines()]
    assert written == payload
    assert process.state["records"] == written
