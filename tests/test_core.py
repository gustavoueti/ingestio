"""Execution semantics of `Process`: ordering, cancellation and shared state."""

from __future__ import annotations

import logging

import pytest

from ingestio import Callback, CancelProcessException, CancelStepException, Process, Step


class Trace(Callback):
    """Records each lifecycle method it receives into the shared state."""

    def __init__(self, label: str) -> None:
        self.label = label

    def _mark(self, moment: str) -> None:
        self.process.state.setdefault("trace", []).append(f"{self.label}.{moment}")

    def before(self) -> None:
        self._mark("before")

    def run(self) -> None:
        self._mark("run")

    def after(self) -> None:
        self._mark("after")


def test_steps_and_callbacks_run_in_declaration_order() -> None:
    process = Process(
        [
            Step("extract", [Trace("read")]),
            Step("bronze", [Trace("lineage"), Trace("write")]),
        ]
    ).run()
    assert process.state["trace"] == [
        "read.before",
        "read.run",
        "read.after",
        "lineage.before",
        "write.before",
        "lineage.run",
        "write.run",
        "lineage.after",
        "write.after",
    ]


def test_state_is_shared_between_callbacks() -> None:
    class Produce(Callback):
        def run(self) -> None:
            self.process.state["records"] = [{"id": 1}]

    class Consume(Callback):
        def run(self) -> None:
            self.process.state["seen"] = len(self.process.state["records"])

    process = Process([Step("extract", [Produce()]), Step("load", [Consume()])]).run()
    assert process.state["seen"] == 1


def test_initial_state_is_available_to_the_first_callback() -> None:
    class ReadSeed(Callback):
        def run(self) -> None:
            self.process.state["echo"] = self.process.state["seed"]

    process = Process([Step("start", [ReadSeed()])], state={"seed": "value"}).run()
    assert process.state["echo"] == "value"


def test_cancel_step_skips_the_rest_of_the_step_but_continues_the_process() -> None:
    class Stop(Callback):
        def run(self) -> None:
            raise CancelStepException("nothing to ingest")

    process = Process(
        [
            Step("bronze", [Trace("first"), Stop(), Trace("never_runs")]),
            Step("silver", [Trace("next_step")]),
        ]
    ).run()

    trace = process.state["trace"]
    assert "never_runs.run" not in trace
    # `after` still runs for every callback of the cancelled step.
    assert "first.after" in trace and "never_runs.after" in trace
    # The following step runs normally.
    assert "next_step.run" in trace
    assert not process.cancelled


def test_cancel_process_stops_cleanly_and_still_runs_after() -> None:
    class Stop(Callback):
        def run(self) -> None:
            raise CancelProcessException("source unchanged")

    process = Process(
        [
            Step("bronze", [Trace("cleanup"), Stop()]),
            Step("silver", [Trace("never_runs")]),
        ]
    ).run()

    trace = process.state["trace"]
    assert process.cancelled
    assert "cleanup.after" in trace
    assert not any(entry.startswith("never_runs") for entry in trace)


def test_unhandled_exception_propagates_and_after_still_runs() -> None:
    class Explode(Callback):
        def run(self) -> None:
            raise ValueError("merge key missing")

    process = Process([Step("silver", [Trace("cleanup"), Explode()])])
    with pytest.raises(ValueError, match="merge key missing"):
        process.run()
    assert "cleanup.after" in process.state["trace"]


def test_duplicate_step_names_are_rejected_before_anything_runs() -> None:
    with pytest.raises(ValueError, match="duplicate step name"):
        Process([Step("bronze", [Trace("a")]), Step("bronze", [Trace("b")])])


def test_empty_process_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least one step"):
        Process([])


def test_step_name_must_not_be_blank() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        Step("  ")


def test_a_step_may_hold_no_callbacks() -> None:
    assert Process([Step("placeholder")]).run().state == {}


def test_run_id_is_stable_when_supplied_and_appears_in_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO, logger="ingestio.core"):
        process = Process([Step("bronze", [Trace("x")])], run_id="abc123").run()
    assert process.run_id == "abc123"
    assert "abc123" in caplog.text
    assert "process started" in caplog.text
    assert "bronze" in caplog.text
