"""The progress tree a run writes to its log, and `log_to_console`, which shows it on a stream.

Covers `specs/run_progress_log.md`. Durations change from run to run, so lines are compared
with every duration replaced by `<d>`, except where a fake `time.perf_counter` makes them exact.
Every test that calls `log_to_console` uses the `ingestio_logger` fixture, which restores the
handlers, the level and `propagate` of the `ingestio` logger afterwards.
"""

from __future__ import annotations

import io
import logging
import re
import sys
import time
from collections.abc import Callable, Iterator
from typing import TextIO, cast

import pytest

import ingestio
from ingestio import Callback, CancelProcessException, CancelStepException, Process, Step

Action = Callable[[], None]

DURATION = re.compile(r"\b(?:\d+h \d+m \d+s|\d+m \d+s|\d+\.\d\ds|\d+ms)\b")
MOMENT_LINE = re.compile(r"\[[^\]]+\] │ │ ")
SGR = re.compile(r"\x1b\[[0-9;]*m")
RESET = "\x1b[0m"
DIM = "\x1b[2m"
RED = "\x1b[31m"
GREEN = "\x1b[32m"
YELLOW = "\x1b[33m"
CYAN = "\x1b[36m"

AT001_LINES = [
    "[a3f7c9] ┌ process started · 2 steps",
    "[a3f7c9] │ ┌ extract (1/2) · 1 callback",
    "[a3f7c9] │ │ ✔ ReadSource.run <d>",
    "[a3f7c9] │ └ extract done in <d>",
    "[a3f7c9] │ ┌ load (2/2) · 2 callbacks",
    "[a3f7c9] │ │ ✔ AddLineageColumns.run <d>",
    "[a3f7c9] │ │ ✔ WriteTarget.run <d>",
    "[a3f7c9] │ └ load done in <d>",
    "[a3f7c9] └ process finished in <d>",
]

AT007_ERROR_LINES = [
    "[a3f7c9] │ │ ✖ WriteTarget.run failed after <d>: PermissionError: denied",
    "[a3f7c9] │ └ load failed in <d>",
    "[a3f7c9] └ process failed in <d>",
]

MIXED_LINES = [
    "[r1] ┌ process started · 3 steps",
    "[r1] │ ┌ ok (1/3) · 1 callback",
    "[r1] │ │ ✔ Fine.run <d>",
    "[r1] │ └ ok done in <d>",
    "[r1] │ ┌ skip (2/3) · 1 callback",
    "[r1] │ │ ⊘ Skip.run cancelled the step after <d>: nothing to do",
    "[r1] │ └ skip cancelled in <d>",
    "[r1] │ ┌ boom (3/3) · 1 callback",
    "[r1] │ │ ✖ Boom.run failed after <d>: ValueError: broken",
    "[r1] │ └ boom failed in <d>",
    "[r1] └ process failed in <d>",
]


# --- Test doubles -------------------------------------------------------------------------


def _nothing() -> None:
    """Do nothing: what a moment does unless a test says otherwise."""


def _raises(exc: BaseException) -> Action:
    """An action that raises `exc`."""

    def action() -> None:
        raise exc

    return action


class _Scripted(Callback):
    """A callback shown under `label`, implementing no moment.

    Its subclasses implement some moments, each performing the action given for it.
    """

    def __init__(
        self,
        label: str,
        *,
        before: Action = _nothing,
        run: Action = _nothing,
        after: Action = _nothing,
    ) -> None:
        self.label = label
        self.on_before = before
        self.on_run = run
        self.on_after = after

    @property
    def name(self) -> str:
        return self.label


class _Run(_Scripted):
    """Implements `run` only."""

    def run(self) -> None:
        self.on_run()


class _After(_Scripted):
    """Implements `after` only."""

    def after(self) -> None:
        self.on_after()


class _BeforeRun(_Scripted):
    """Implements `before` and `run`."""

    def before(self) -> None:
        self.on_before()

    def run(self) -> None:
        self.on_run()


class _RunAfter(_Scripted):
    """Implements `run` and `after`."""

    def run(self) -> None:
        self.on_run()

    def after(self) -> None:
        self.on_after()


class _AllMoments(_Scripted):
    """Implements `before`, `run` and `after`."""

    def before(self) -> None:
        self.on_before()

    def run(self) -> None:
        self.on_run()

    def after(self) -> None:
        self.on_after()


class _Clock:
    """Stands in for `time.perf_counter`: time moves only when a callback advances it."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> Action:
        """An action that moves the clock forward by `seconds`."""

        def action() -> None:
            self.now += seconds

        return action


class _Terminal(io.StringIO):
    """An in-memory stream that says it is a terminal."""

    def isatty(self) -> bool:
        return True


class _Sink:
    """A stream with `write` and `flush` only, and no `isatty`."""

    def __init__(self) -> None:
        self.parts: list[str] = []

    def write(self, text: str) -> int:
        self.parts.append(text)
        return len(text)

    def flush(self) -> None:
        """Nothing to flush."""


# --- Fixtures and helpers -----------------------------------------------------------------


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    """Replace `time.perf_counter` with a clock that only the test's callbacks advance."""
    fake = _Clock()
    monkeypatch.setattr(time, "perf_counter", fake)
    return fake


@pytest.fixture
def engine_log(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """`caplog`, capturing what the engine writes to `ingestio.core` from `INFO` up."""
    caplog.set_level(logging.INFO, logger="ingestio.core")
    return caplog


@pytest.fixture
def ingestio_logger() -> Iterator[logging.Logger]:
    """The `ingestio` logger, with its handlers, level and `propagate` restored afterwards."""
    logger = logging.getLogger("ingestio")
    handlers = list(logger.handlers)
    level = logger.level
    propagate = logger.propagate
    try:
        yield logger
    finally:
        logger.handlers[:] = handlers
        logger.setLevel(level)
        logger.propagate = propagate


def _records(
    caplog: pytest.LogCaptureFixture, logger: str = "ingestio.core"
) -> list[logging.LogRecord]:
    """The records written to `logger`, in order."""
    return [record for record in caplog.records if record.name == logger]


def _messages(caplog: pytest.LogCaptureFixture, logger: str = "ingestio.core") -> list[str]:
    """The messages of the records written to `logger`, in order."""
    return [record.getMessage() for record in _records(caplog, logger)]


def _levels(caplog: pytest.LogCaptureFixture, logger: str = "ingestio.core") -> list[int]:
    """The levels of the records written to `logger`, in order."""
    return [record.levelno for record in _records(caplog, logger)]


def _shape(text: str) -> str:
    """`text` with every duration replaced by `<d>`."""
    return DURATION.sub("<d>", text)


def _lines(caplog: pytest.LogCaptureFixture, logger: str = "ingestio.core") -> list[str]:
    """The messages written to `logger`, with every duration replaced by `<d>`."""
    return [_shape(message) for message in _messages(caplog, logger)]


def _moment_lines(lines: list[str]) -> list[str]:
    """The lines of callback moments, those at depth 2."""
    return [line for line in lines if MOMENT_LINE.match(line)]


def _text(lines: list[str]) -> str:
    """What a stream holds once `lines` were written to it, each followed by a line break."""
    return "".join(f"{line}\n" for line in lines)


def _at001_process(*, write: Action = _nothing, log: logging.Logger | None = None) -> Process:
    """The process of AT-001. `write` is what `WriteTarget.run` does."""
    return Process(
        [
            Step("extract", [_Run("ReadSource")]),
            Step("load", [_Run("AddLineageColumns"), _Run("WriteTarget", run=write)]),
        ],
        run_id="a3f7c9",
        log=log,
    )


def _mixed_process(failure: Exception) -> Process:
    """A process whose first step succeeds, second is cancelled and third raises `failure`."""
    return Process(
        [
            Step("ok", [_Run("Fine")]),
            Step("skip", [_Run("Skip", run=_raises(CancelStepException("nothing to do")))]),
            Step("boom", [_Run("Boom", run=_raises(failure))]),
        ],
        run_id="r1",
    )


def _assert_colored(line: str, marker: str, color: str, word: str | None) -> None:
    """Check the colors of one line the helper wrote.

    The line starts with the dim prefix and `color` comes right before `marker`. Given a
    `word`, the last SGR sequence before it is `color`; otherwise the text after the marker
    holds no SGR sequence other than resets.
    """
    assert line.startswith(f"{DIM}["), line
    at = line.index(marker)
    assert line[:at].endswith(color), line
    if word is None:
        assert all(code == RESET for code in SGR.findall(line[at + 1 :])), line
    else:
        assert SGR.findall(line[: line.index(word, at)])[-1] == color, line


# --- The tree -----------------------------------------------------------------------------


def test_at001_a_run_writes_a_tree_of_its_steps_and_callback_moments(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    _at001_process().run()
    assert _lines(engine_log) == AT001_LINES
    assert _levels(engine_log) == [logging.INFO] * 9


def test_at002_moment_lines_follow_the_passes_then_the_declaration_order(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    Process([Step("load", [_AllMoments("First"), _AllMoments("Second")])], run_id="r1").run()
    assert _lines(engine_log) == [
        "[r1] ┌ process started · 1 step",
        "[r1] │ ┌ load (1/1) · 2 callbacks",
        "[r1] │ │ ✔ First.before <d>",
        "[r1] │ │ ✔ Second.before <d>",
        "[r1] │ │ ✔ First.run <d>",
        "[r1] │ │ ✔ Second.run <d>",
        "[r1] │ │ ✔ First.after <d>",
        "[r1] │ │ ✔ Second.after <d>",
        "[r1] │ └ load done in <d>",
        "[r1] └ process finished in <d>",
    ]


def test_at003_a_callback_implementing_only_after_gets_only_an_after_line(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    Process([Step("s", [_After("Late")])], run_id="r1").run()
    assert _moment_lines(_lines(engine_log)) == ["[r1] │ │ ✔ Late.after <d>"]


def test_at003_a_callback_implementing_no_moment_gets_no_line(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    Process([Step("s", [_Scripted("Idle")])], run_id="r1").run()
    assert _lines(engine_log) == [
        "[r1] ┌ process started · 1 step",
        "[r1] │ ┌ s (1/1) · 1 callback",
        "[r1] │ └ s done in <d>",
        "[r1] └ process finished in <d>",
    ]


def test_at004_a_moment_inherited_from_a_parent_class_gets_a_line(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    class Base(Callback):
        def run(self) -> None:
            """Inherited by `Child`."""

    class Child(Base):
        def before(self) -> None:
            """Implemented by `Child` itself."""

    Process([Step("s", [Child()])], run_id="r1").run()
    assert _moment_lines(_lines(engine_log)) == [
        "[r1] │ │ ✔ Child.before <d>",
        "[r1] │ │ ✔ Child.run <d>",
    ]


def test_at005_a_line_shows_the_callback_name(engine_log: pytest.LogCaptureFixture) -> None:
    class StripColumnSuffix(Callback):
        @property
        def name(self) -> str:
            return "strip_column_suffix"

        def run(self) -> None:
            """Does the work."""

    Process([Step("s", [StripColumnSuffix()])], run_id="r1").run()
    assert _moment_lines(_lines(engine_log)) == ["[r1] │ │ ✔ strip_column_suffix.run <d>"]


def test_at006_a_step_without_callbacks_writes_its_start_and_closing_lines(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    Process([Step("placeholder")], run_id="r1").run()
    assert _lines(engine_log) == [
        "[r1] ┌ process started · 1 step",
        "[r1] │ ┌ placeholder (1/1) · 0 callbacks",
        "[r1] │ └ placeholder done in <d>",
        "[r1] └ process finished in <d>",
    ]


# --- Failure ------------------------------------------------------------------------------


def test_at007_a_failure_closes_the_moment_the_step_and_the_process_at_error(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    with pytest.raises(PermissionError, match="denied"):
        _at001_process(write=_raises(PermissionError("denied"))).run()
    lines = _lines(engine_log)
    assert lines == AT001_LINES[:6] + AT007_ERROR_LINES
    assert _levels(engine_log) == [logging.INFO] * 6 + [logging.ERROR] * 3
    assert not any("process finished" in line for line in lines)


def test_at008_a_failing_before_skips_the_run_line(engine_log: pytest.LogCaptureFixture) -> None:
    check = _BeforeRun("Check", before=_raises(ValueError("bad")))
    with pytest.raises(ValueError, match="bad"):
        Process([Step("s", [check])], run_id="r1").run()
    assert _lines(engine_log) == [
        "[r1] ┌ process started · 1 step",
        "[r1] │ ┌ s (1/1) · 1 callback",
        "[r1] │ │ ✖ Check.before failed after <d>: ValueError: bad",
        "[r1] │ └ s failed in <d>",
        "[r1] └ process failed in <d>",
    ]
    assert _levels(engine_log)[2:] == [logging.ERROR] * 3
    assert not any("Check.run" in line for line in _lines(engine_log))


def test_at008_a_failing_after_closes_the_step_and_the_process_as_failed(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    check = _After("Check", after=_raises(ValueError("late")))
    with pytest.raises(ValueError, match="late"):
        Process([Step("s", [check])], run_id="r1").run()
    assert _lines(engine_log)[2:] == [
        "[r1] │ │ ✖ Check.after failed after <d>: ValueError: late",
        "[r1] │ └ s failed in <d>",
        "[r1] └ process failed in <d>",
    ]
    assert _levels(engine_log)[2:] == [logging.ERROR] * 3


def test_at009_after_lines_follow_the_failure_of_a_run(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    a = _RunAfter("A")
    b = _Run("B", run=_raises(ValueError("boom")))
    with pytest.raises(ValueError, match="boom"):
        Process([Step("s", [a, b])], run_id="r1").run()
    assert _lines(engine_log) == [
        "[r1] ┌ process started · 1 step",
        "[r1] │ ┌ s (1/1) · 2 callbacks",
        "[r1] │ │ ✔ A.run <d>",
        "[r1] │ │ ✖ B.run failed after <d>: ValueError: boom",
        "[r1] │ │ ✔ A.after <d>",
        "[r1] │ └ s failed in <d>",
        "[r1] └ process failed in <d>",
    ]


def test_at010_an_empty_failure_message_leaves_the_type_alone(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    with pytest.raises(ValueError):
        Process([Step("s", [_Run("X", run=_raises(ValueError()))])], run_id="r1").run()
    assert _moment_lines(_lines(engine_log)) == ["[r1] │ │ ✖ X.run failed after <d>: ValueError"]


def test_at010_an_empty_step_cancellation_reason_ends_the_line_at_the_duration(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    Process([Step("s", [_Run("X", run=_raises(CancelStepException()))])], run_id="r1").run()
    assert _moment_lines(_lines(engine_log)) == ["[r1] │ │ ⊘ X.run cancelled the step after <d>"]


def test_at010_an_empty_process_cancellation_reason_ends_the_line_at_the_duration(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    Process([Step("s", [_Run("X", run=_raises(CancelProcessException()))])], run_id="r1").run()
    assert _moment_lines(_lines(engine_log)) == ["[r1] │ │ ⊘ X.run cancelled the process after <d>"]


def test_at011_a_message_on_several_lines_continues_under_the_run_id_and_guides(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    failure = ValueError("first line\nsecond line")
    with pytest.raises(ValueError, match="first line"):
        Process([Step("s", [_Run("X", run=_raises(failure))])], run_id="r1").run()
    assert _moment_lines(_lines(engine_log)) == [
        "[r1] │ │ ✖ X.run failed after <d>: ValueError: first line\n[r1] │ │   second line"
    ]
    assert all(
        line.startswith("[r1] ")
        for message in _messages(engine_log)
        for line in message.splitlines()
    )


def test_at012_the_exception_leaves_run_untouched_and_no_record_carries_it(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    err = ValueError("merge key missing")
    process = Process([Step("s", [_Run("X", run=_raises(err))])], run_id="r1")
    with pytest.raises(ValueError, match="merge key missing") as raised:
        process.run()
    assert raised.value is err
    assert not hasattr(err, "__notes__")
    # The failure lines are the records most likely to carry the exception: they must exist.
    assert _lines(engine_log)[-3:] == [
        "[r1] │ │ ✖ X.run failed after <d>: ValueError: merge key missing",
        "[r1] │ └ s failed in <d>",
        "[r1] └ process failed in <d>",
    ]
    assert not any(record.exc_info for record in _records(engine_log))


# --- Cancellation -------------------------------------------------------------------------


def test_at013_a_cancelled_step_is_closed_as_cancelled_and_the_process_goes_on(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    first = _RunAfter("First")
    stop = _Run("Stop", run=_raises(CancelStepException("nothing to ingest")))
    nxt = _Run("Next")
    Process([Step("bronze", [first, stop]), Step("silver", [nxt])], run_id="r1").run()
    assert _lines(engine_log) == [
        "[r1] ┌ process started · 2 steps",
        "[r1] │ ┌ bronze (1/2) · 2 callbacks",
        "[r1] │ │ ✔ First.run <d>",
        "[r1] │ │ ⊘ Stop.run cancelled the step after <d>: nothing to ingest",
        "[r1] │ │ ✔ First.after <d>",
        "[r1] │ └ bronze cancelled in <d>",
        "[r1] │ ┌ silver (2/2) · 1 callback",
        "[r1] │ │ ✔ Next.run <d>",
        "[r1] │ └ silver done in <d>",
        "[r1] └ process finished in <d>",
    ]
    assert _levels(engine_log) == [logging.INFO] * 10


def test_at014_a_cancelled_process_closes_as_cancelled_and_skips_later_steps(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    stop = _Run("Stop", run=_raises(CancelProcessException("source unchanged")))
    Process([Step("bronze", [stop]), Step("silver", [_Run("Next")])], run_id="r1").run()
    lines = _lines(engine_log)
    assert "[r1] │ │ ⊘ Stop.run cancelled the process after <d>: source unchanged" in lines
    assert lines[-2:] == ["[r1] │ └ bronze cancelled in <d>", "[r1] └ process cancelled in <d>"]
    assert not any("silver" in line for line in lines)
    assert all(level <= logging.INFO for level in _levels(engine_log))


def test_at015_an_after_cancelling_the_process_closes_everything_as_cancelled(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    stop = _After("X", after=_raises(CancelProcessException("stop")))
    process = Process([Step("s", [stop])], run_id="r1").run()
    lines = _lines(engine_log)
    assert process.cancelled
    assert "[r1] │ │ ⊘ X.after cancelled the process after <d>: stop" in lines
    assert lines[-2:] == ["[r1] │ └ s cancelled in <d>", "[r1] └ process cancelled in <d>"]
    assert all(level <= logging.INFO for level in _levels(engine_log))


def test_at016_a_step_cancellation_raised_from_after_is_a_failure(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    late = _After("X", after=_raises(CancelStepException("too late")))
    with pytest.raises(CancelStepException, match="too late"):
        Process([Step("s", [late])], run_id="r1").run()
    assert _lines(engine_log)[-3:] == [
        "[r1] │ │ ✖ X.after failed after <d>: CancelStepException: too late",
        "[r1] │ └ s failed in <d>",
        "[r1] └ process failed in <d>",
    ]
    assert _levels(engine_log)[-3:] == [logging.ERROR] * 3


# --- Durations ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("seconds", "text"),
    [
        (0, "0ms"),
        (0.0004, "0ms"),
        (0.4125, "412ms"),
        (0.9995, "999ms"),
        (1, "1.00s"),
        (3.5289, "3.52s"),
        (59.9995, "59.99s"),
        (60, "1m 0s"),
        (133.9, "2m 13s"),
        (3723.5, "1h 2m 3s"),
    ],
)
def test_at017_a_duration_is_written_in_the_unit_that_fits_and_truncated(
    clock: _Clock, engine_log: pytest.LogCaptureFixture, seconds: float, text: str
) -> None:
    Process([Step("s", [_Run("X", run=clock.advance(seconds))])], run_id="r1").run()
    assert _moment_lines(_messages(engine_log)) == [f"[r1] │ │ ✔ X.run {text}"]


def test_at018_steps_and_the_process_cover_the_time_of_their_moments(
    clock: _Clock, engine_log: pytest.LogCaptureFixture
) -> None:
    x = _AllMoments("X", before=clock.advance(1), run=clock.advance(2), after=clock.advance(3))
    y = _Run("Y", run=clock.advance(0.5))
    Process([Step("s1", [x]), Step("s2", [y])], run_id="r1").run()
    assert _messages(engine_log) == [
        "[r1] ┌ process started · 2 steps",
        "[r1] │ ┌ s1 (1/2) · 1 callback",
        "[r1] │ │ ✔ X.before 1.00s",
        "[r1] │ │ ✔ X.run 2.00s",
        "[r1] │ │ ✔ X.after 3.00s",
        "[r1] │ └ s1 done in 6.00s",
        "[r1] │ ┌ s2 (2/2) · 1 callback",
        "[r1] │ │ ✔ Y.run 500ms",
        "[r1] │ └ s2 done in 500ms",
        "[r1] └ process finished in 6.50s",
    ]


# --- Concurrent runs and injected loggers -------------------------------------------------


def test_at019_a_process_run_inside_a_callback_writes_its_own_tree(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    def run_inner() -> None:
        Process([Step("inner_step", [_Run("Y")])], run_id="in").run()

    Process([Step("outer_step", [_Run("Outer", run=run_inner)])], run_id="out").run()

    inner = [
        "[in] ┌ process started · 1 step",
        "[in] │ ┌ inner_step (1/1) · 1 callback",
        "[in] │ │ ✔ Y.run <d>",
        "[in] │ └ inner_step done in <d>",
        "[in] └ process finished in <d>",
    ]
    outer = [
        "[out] ┌ process started · 1 step",
        "[out] │ ┌ outer_step (1/1) · 1 callback",
        "[out] │ │ ✔ Outer.run <d>",
        "[out] │ └ outer_step done in <d>",
        "[out] └ process finished in <d>",
    ]
    lines = _lines(engine_log)
    assert [line for line in lines if line.startswith("[in] ")] == inner
    assert [line for line in lines if line.startswith("[out] ")] == outer
    assert lines == outer[:2] + inner + outer[2:]


def test_at020_an_injected_logger_receives_the_tree_instead_of_ingestio_core(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    engine_log.set_level(logging.INFO, logger="my.app")
    _at001_process(log=logging.getLogger("my.app")).run()
    assert _lines(engine_log, "my.app") == AT001_LINES
    assert _levels(engine_log, "my.app") == [logging.INFO] * 9
    assert _records(engine_log, "ingestio.core") == []


# --- The helper ---------------------------------------------------------------------------


def test_at021_log_to_console_is_exported_by_the_package() -> None:
    from ingestio import log_to_console

    assert callable(log_to_console)
    assert "log_to_console" in ingestio.__all__


def test_at022_the_helper_writes_the_bare_tree_to_its_stream(
    caplog: pytest.LogCaptureFixture, ingestio_logger: logging.Logger
) -> None:
    caplog.set_level(logging.WARNING)
    ingestio_logger.setLevel(logging.NOTSET)
    stream = io.StringIO()
    ingestio.log_to_console(stream=stream)
    _at001_process().run()
    text = stream.getvalue()
    assert _shape(text) == _text(AT001_LINES)
    assert "\x1b" not in text


def test_at023_removing_the_returned_handler_undoes_the_call(
    ingestio_logger: logging.Logger,
) -> None:
    stream = io.StringIO()
    handler = ingestio.log_to_console(stream=stream)
    assert isinstance(handler, logging.Handler)
    assert handler in ingestio_logger.handlers
    ingestio_logger.removeHandler(handler)
    _at001_process().run()
    assert stream.getvalue() == ""


def test_at024_the_last_call_replaces_the_handler_of_the_previous_one(
    ingestio_logger: logging.Logger,
) -> None:
    before = list(ingestio_logger.handlers)
    a = io.StringIO()
    b = io.StringIO()
    first = ingestio.log_to_console(stream=a)
    second = ingestio.log_to_console(stream=b)
    assert len(ingestio_logger.handlers) == len(before) + 1
    assert set(ingestio_logger.handlers) == {*before, second}
    assert first not in ingestio_logger.handlers
    _at001_process().run()
    assert _shape(b.getvalue()) == _text(AT001_LINES)
    assert a.getvalue() == ""


def test_at025_the_helper_leaves_other_handlers_the_root_logger_and_propagation_alone(
    engine_log: pytest.LogCaptureFixture, ingestio_logger: logging.Logger
) -> None:
    application = logging.StreamHandler(io.StringIO())
    ingestio_logger.addHandler(application)
    package = [h for h in ingestio_logger.handlers if isinstance(h, logging.NullHandler)]
    root = logging.getLogger()
    root_handlers = list(root.handlers)
    root_level = root.level
    propagate = ingestio_logger.propagate
    assert package

    ingestio.log_to_console(stream=io.StringIO())

    assert application in ingestio_logger.handlers
    assert all(handler in ingestio_logger.handlers for handler in package)
    assert root.handlers == root_handlers
    assert root.level == root_level
    assert ingestio_logger.propagate == propagate
    _at001_process().run()
    assert _lines(engine_log) == AT001_LINES


def test_at026_the_helper_lowers_the_ingestio_level_so_info_lines_reach_it(
    caplog: pytest.LogCaptureFixture, ingestio_logger: logging.Logger
) -> None:
    caplog.set_level(logging.WARNING)
    ingestio_logger.setLevel(logging.NOTSET)
    stream = io.StringIO()
    handler = ingestio.log_to_console(stream=stream)
    assert ingestio_logger.level == logging.INFO
    assert handler.level == logging.INFO
    _at001_process().run()
    assert _shape(stream.getvalue()) == _text(AT001_LINES)


def test_at026_the_helper_never_raises_the_ingestio_level(
    ingestio_logger: logging.Logger,
) -> None:
    ingestio_logger.setLevel(logging.DEBUG)
    ingestio.log_to_console(stream=io.StringIO())
    assert ingestio_logger.level == logging.DEBUG


@pytest.mark.usefixtures("ingestio_logger")
def test_at026_the_handler_writes_only_records_from_its_level_up() -> None:
    stream = io.StringIO()
    ingestio.log_to_console(stream=stream, level=logging.ERROR)
    with pytest.raises(PermissionError, match="denied"):
        _at001_process(write=_raises(PermissionError("denied"))).run()
    assert _shape(stream.getvalue()) == _text(AT007_ERROR_LINES)


@pytest.mark.usefixtures("ingestio_logger")
def test_at027_without_a_stream_the_helper_writes_to_stderr_as_it_is_when_called(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stderr = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stderr)
    ingestio.log_to_console()
    _at001_process().run()
    assert _shape(stderr.getvalue()) == _text(AT001_LINES)


@pytest.mark.parametrize(
    ("color", "terminal", "no_color", "colored"),
    [
        (None, False, None, False),
        (None, True, None, True),
        (None, True, "1", False),
        (None, True, "", True),
        (False, True, None, False),
        (True, False, None, True),
        (True, True, "1", True),
    ],
)
@pytest.mark.usefixtures("ingestio_logger")
def test_at028_color_follows_the_argument_then_no_color_then_the_terminal(
    monkeypatch: pytest.MonkeyPatch,
    color: bool | None,
    terminal: bool,
    no_color: str | None,
    colored: bool,
) -> None:
    if no_color is None:
        monkeypatch.delenv("NO_COLOR", raising=False)
    else:
        monkeypatch.setenv("NO_COLOR", no_color)
    stream = _Terminal() if terminal else io.StringIO()
    ingestio.log_to_console(stream=stream, color=color)
    _at001_process().run()
    assert stream.getvalue()
    assert ("\x1b[" in stream.getvalue()) is colored


@pytest.mark.usefixtures("ingestio_logger")
def test_at029_removing_the_colors_gives_back_the_messages_of_the_records(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    stream = io.StringIO()
    ingestio.log_to_console(stream=stream, color=True)
    with pytest.raises(ValueError, match="first line"):
        _mixed_process(ValueError("first line\nsecond line")).run()
    text = stream.getvalue()
    assert SGR.sub("", text) == _text(_messages(engine_log))
    colored = [line for line in text.split("\n") if SGR.search(line)]
    assert colored
    assert all(SGR.findall(line)[-1] == RESET for line in colored)


@pytest.mark.usefixtures("ingestio_logger")
def test_at030_markers_and_outcome_text_take_the_color_of_the_outcome() -> None:
    stream = io.StringIO()
    ingestio.log_to_console(stream=stream, color=True)
    with pytest.raises(ValueError, match="broken"):
        _mixed_process(ValueError("broken")).run()
    lines = stream.getvalue().splitlines()
    assert [_shape(SGR.sub("", line)) for line in lines] == MIXED_LINES
    # For each line: its marker, the color right before it, and the word that carries that
    # color on failure and cancellation lines (None where the text has no color).
    expected: list[tuple[str, str, str | None]] = [
        ("┌", CYAN, None),
        ("┌", CYAN, None),
        ("✔", GREEN, None),
        ("└", GREEN, None),
        ("┌", CYAN, None),
        ("⊘", YELLOW, "cancelled"),
        ("└", YELLOW, "cancelled"),
        ("┌", CYAN, None),
        ("✖", RED, "failed"),
        ("└", RED, "failed"),
        ("└", RED, "failed"),
    ]
    for line, (marker, color, word) in zip(lines, expected, strict=True):
        _assert_colored(line, marker, color, word)


@pytest.mark.usefixtures("ingestio_logger")
def test_at030_the_finished_line_of_a_successful_process_is_green() -> None:
    stream = io.StringIO()
    ingestio.log_to_console(stream=stream, color=True)
    _at001_process().run()
    last = stream.getvalue().splitlines()[-1]
    assert _shape(SGR.sub("", last)) == AT001_LINES[-1]
    _assert_colored(last, "└", GREEN, None)


@pytest.mark.usefixtures("ingestio_logger")
def test_at031_records_never_carry_color_codes(engine_log: pytest.LogCaptureFixture) -> None:
    stream = io.StringIO()
    ingestio.log_to_console(stream=stream, color=True)
    with pytest.raises(ValueError, match="broken"):
        _mixed_process(ValueError("broken")).run()
    assert "\x1b[" in stream.getvalue()
    assert engine_log.records
    assert not any("\x1b" in record.getMessage() for record in engine_log.records)


@pytest.mark.usefixtures("ingestio_logger")
def test_at032_other_records_are_written_as_their_bare_message_without_color() -> None:
    stream = io.StringIO()
    ingestio.log_to_console(stream=stream, color=True)
    logging.getLogger("ingestio.custom").warning("hello")
    assert stream.getvalue() == "hello\n"


# --- Building a line never raises ---------------------------------------------------------


def test_at033_an_exception_whose_str_raises_is_shown_by_its_type_alone(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    class BadStr(Exception):  # noqa: N818 - the name the criterion's line shows
        def __str__(self) -> str:
            raise RuntimeError("no text")

    err = BadStr("unreadable")
    with pytest.raises(BadStr) as raised:
        Process([Step("s", [_Run("X", run=_raises(err))])], run_id="r1").run()
    assert raised.value is err
    assert _lines(engine_log)[-3:] == [
        "[r1] │ │ ✖ X.run failed after <d>: BadStr",
        "[r1] │ └ s failed in <d>",
        "[r1] └ process failed in <d>",
    ]
    assert _levels(engine_log)[-3:] == [logging.ERROR] * 3


def test_at034_a_step_cancellation_whose_str_raises_ends_its_line_at_the_duration(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    class BadStep(CancelStepException):
        def __str__(self) -> str:
            raise RuntimeError("no text")

    process = Process([Step("s", [_Run("X", run=_raises(BadStep("unreadable")))])], run_id="r1")
    assert process.run() is process
    assert _lines(engine_log) == [
        "[r1] ┌ process started · 1 step",
        "[r1] │ ┌ s (1/1) · 1 callback",
        "[r1] │ │ ⊘ X.run cancelled the step after <d>",
        "[r1] │ └ s cancelled in <d>",
        "[r1] └ process finished in <d>",
    ]


def test_at034_a_process_cancellation_whose_str_raises_ends_its_line_at_the_duration(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    class BadStop(CancelProcessException):
        def __str__(self) -> str:
            raise RuntimeError("no text")

    process = Process([Step("s", [_Run("X", run=_raises(BadStop("unreadable")))])], run_id="r1")
    assert process.run() is process
    assert process.cancelled
    assert _lines(engine_log) == [
        "[r1] ┌ process started · 1 step",
        "[r1] │ ┌ s (1/1) · 1 callback",
        "[r1] │ │ ⊘ X.run cancelled the process after <d>",
        "[r1] │ └ s cancelled in <d>",
        "[r1] └ process cancelled in <d>",
    ]


def test_at035_a_name_that_raises_is_replaced_by_the_class_name(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    class Unnamed(Callback):
        @property
        def name(self) -> str:
            raise RuntimeError("no name")

        def run(self) -> None:
            """Does the work."""

    process = Process([Step("s", [Unnamed()])], run_id="r1")
    assert process.run() is process
    lines = _lines(engine_log)
    assert _moment_lines(lines) == ["[r1] │ │ ✔ Unnamed.run <d>"]
    assert lines[-1] == "[r1] └ process finished in <d>"


def test_at036_a_name_that_raises_leaves_the_callback_exception_untouched(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    err = ValueError("boom")

    class Unnamed(Callback):
        @property
        def name(self) -> str:
            raise RuntimeError("no name")

        def run(self) -> None:
            raise err

    with pytest.raises(ValueError, match="boom") as raised:
        Process([Step("s", [Unnamed()])], run_id="r1").run()
    assert raised.value is err
    written = list(zip(_lines(engine_log), _levels(engine_log), strict=True))
    assert ("[r1] │ │ ✖ Unnamed.run failed after <d>: ValueError: boom", logging.ERROR) in written


# --- Helper arguments ---------------------------------------------------------------------


def test_at037_the_helper_rejects_a_level_that_is_not_above_notset(
    ingestio_logger: logging.Logger,
) -> None:
    ingestio_logger.setLevel(logging.WARNING)
    handlers = list(ingestio_logger.handlers)
    with pytest.raises(ValueError, match=r"^level must be above NOTSET$"):
        ingestio.log_to_console(stream=io.StringIO(), level=logging.NOTSET)
    assert ingestio_logger.handlers == handlers
    assert ingestio_logger.level == logging.WARNING


# --- Moments assigned on the instance -----------------------------------------------------


def test_at038_a_moment_assigned_on_the_instance_gets_a_line(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    class X(Callback):
        def __init__(self) -> None:
            self.worked = False
            self.run = self._work  # type: ignore[method-assign]

        def _work(self) -> None:
            self.worked = True

    x = X()
    Process([Step("s", [x])], run_id="r1").run()
    assert x.worked
    assert _moment_lines(_lines(engine_log)) == ["[r1] │ │ ✔ X.run <d>"]


# --- Behaviour the spec pins beyond its acceptance criteria -------------------------------


def test_a_moment_set_back_to_the_base_method_gets_no_line(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    class Base(Callback):
        def run(self) -> None:
            """Set back to `Callback.run` by `Quiet`."""

    class Quiet(Base):
        run = Callback.run

    Process([Step("s", [Quiet()])], run_id="r1").run()
    assert _lines(engine_log) == [
        "[r1] ┌ process started · 1 step",
        "[r1] │ ┌ s (1/1) · 1 callback",
        "[r1] │ └ s done in <d>",
        "[r1] └ process finished in <d>",
    ]


def test_an_override_gets_a_line_even_when_it_does_nothing_or_only_calls_super(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    class Echo(Callback):
        def run(self) -> None:
            super().run()

        def after(self) -> None:
            """Does nothing."""

    Process([Step("s", [Echo()])], run_id="r1").run()
    assert _moment_lines(_lines(engine_log)) == [
        "[r1] │ │ ✔ Echo.run <d>",
        "[r1] │ │ ✔ Echo.after <d>",
    ]


def test_an_exception_outside_exception_fails_the_moment_like_any_other(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    with pytest.raises(KeyboardInterrupt):
        Process([Step("s", [_Run("X", run=_raises(KeyboardInterrupt()))])], run_id="r1").run()
    assert _lines(engine_log)[2:] == [
        "[r1] │ │ ✖ X.run failed after <d>: KeyboardInterrupt",
        "[r1] │ └ s failed in <d>",
        "[r1] └ process failed in <d>",
    ]
    assert _levels(engine_log)[2:] == [logging.ERROR] * 3


def test_an_after_failing_while_another_exception_leaves_the_step_gets_its_own_line(
    engine_log: pytest.LogCaptureFixture,
) -> None:
    x = _RunAfter("X", run=_raises(ValueError("first")), after=_raises(ValueError("second")))
    with pytest.raises(ValueError):
        Process([Step("s", [x])], run_id="r1").run()
    assert _lines(engine_log) == [
        "[r1] ┌ process started · 1 step",
        "[r1] │ ┌ s (1/1) · 1 callback",
        "[r1] │ │ ✖ X.run failed after <d>: ValueError: first",
        "[r1] │ │ ✖ X.after failed after <d>: ValueError: second",
        "[r1] │ └ s failed in <d>",
        "[r1] └ process failed in <d>",
    ]


@pytest.mark.usefixtures("ingestio_logger")
def test_a_stream_without_isatty_counts_as_not_a_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NO_COLOR", raising=False)
    sink = _Sink()
    ingestio.log_to_console(stream=cast(TextIO, sink))
    _at001_process().run()
    assert _shape("".join(sink.parts)) == _text(AT001_LINES)


@pytest.mark.usefixtures("ingestio_logger")
def test_the_default_stream_is_stderr_as_it_is_when_the_helper_is_called(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    at_call = io.StringIO()
    later = io.StringIO()
    monkeypatch.setattr(sys, "stderr", at_call)
    ingestio.log_to_console()
    monkeypatch.setattr(sys, "stderr", later)
    _at001_process().run()
    assert _shape(at_call.getvalue()) == _text(AT001_LINES)
    assert later.getvalue() == ""


@pytest.mark.usefixtures("ingestio_logger")
def test_color_is_decided_when_the_helper_is_called(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NO_COLOR", raising=False)
    stream = _Terminal()
    ingestio.log_to_console(stream=stream)
    monkeypatch.setenv("NO_COLOR", "1")
    _at001_process().run()
    assert "\x1b[" in stream.getvalue()


@pytest.mark.usefixtures("ingestio_logger")
def test_a_continuation_line_has_a_dim_prefix_and_the_color_of_its_first_line() -> None:
    stream = io.StringIO()
    ingestio.log_to_console(stream=stream, color=True)
    failure = ValueError("first line\nsecond line")
    with pytest.raises(ValueError, match="first line"):
        Process([Step("s", [_Run("X", run=_raises(failure))])], run_id="r1").run()
    continuation = stream.getvalue().splitlines()[3]
    assert SGR.sub("", continuation) == "[r1] │ │   second line"
    assert continuation.startswith(f"{DIM}[")
    assert SGR.findall(continuation[: continuation.index("│")])[-1] == DIM
    assert SGR.findall(continuation[: continuation.index("second line")])[-1] == RED
