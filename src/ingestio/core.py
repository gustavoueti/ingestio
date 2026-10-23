"""`Process` and `Step`: the orchestrator and the points of a process.

`Process` owns the shared state that callbacks read and write, the ordered list of steps,
and the run loop. `Step` names a point of the process and holds the callbacks that run
there.

Steps run in list order, and callbacks within a step run in list order.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ingestio.callback.core import Callback, CancelProcessException, CancelStepException
from ingestio.console import RECORD_KEY, TreeLine

__all__ = ["Process", "Step"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Step:
    """A named point of a process, holding the callbacks that run there."""

    name: str
    callbacks: tuple[Callback, ...] = field(default=())

    def __init__(self, name: str, callbacks: Iterable[Callback] = ()) -> None:
        if not name.strip():
            raise ValueError("step name must be a non-empty string")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "callbacks", tuple(callbacks))


class Process:
    """Runs an ordered list of steps, carrying the state every callback shares.

    Args:
        steps: Steps to run, in order.
        state: Initial shared state.
        run_id: Identifier for this run. Generated when omitted.
        log: Logger to write to.
    """

    def __init__(
        self,
        steps: Iterable[Step],
        *,
        state: Mapping[str, Any] | None = None,
        run_id: str | None = None,
        log: logging.Logger | None = None,
    ) -> None:
        self.steps = self._validated(steps)
        self.state: dict[str, Any] = dict(state or {})
        self.run_id = run_id or uuid.uuid4().hex[:8]
        self.started_at: datetime | None = None
        self.cancelled = False
        self._log = log or logger
        for step in self.steps:
            for callback in step.callbacks:
                callback.process = self

    @staticmethod
    def _validated(steps: Iterable[Step]) -> tuple[Step, ...]:
        """Reject an unusable process before anything runs."""
        ordered = tuple(steps)
        if not ordered:
            raise ValueError("a process needs at least one step")
        seen: set[str] = set()
        for step in ordered:
            if step.name in seen:
                raise ValueError(f"duplicate step name: {step.name!r}")
            seen.add(step.name)
        return ordered

    def run(self) -> Process:
        """Run every step in order. Returns self, so the state stays reachable."""
        self.started_at = datetime.now(UTC)
        self.cancelled = False
        started = time.perf_counter()
        self._emit(
            logging.INFO, 0, "┌", "start", f"process started · {_count(len(self.steps), 'step')}"
        )

        outcome = "finished"
        try:
            for position, step in enumerate(self.steps, start=1):
                self._run_step(step, position)
        except CancelProcessException:
            self.cancelled = True
            outcome = "cancelled"
        except BaseException:
            outcome = "failed"
            raise
        finally:
            # Written before the exception leaves run(), which stays the very object raised.
            self._close(0, f"process {outcome}", started, outcome)
        return self

    def _run_step(self, step: Step, position: int) -> None:
        """Run the step in three passes: every `before`, every `run`, then every `after`.

        `after` runs whether the step completed, was cancelled, or the process is stopping.
        """
        started = time.perf_counter()
        self._emit(
            logging.INFO,
            1,
            "┌",
            "start",
            f"{step.name} ({position}/{len(self.steps)}) · "
            f"{_count(len(step.callbacks), 'callback')}",
        )
        outcome = "done"
        try:
            try:
                for callback in step.callbacks:
                    self._moment(callback, "before")
                for callback in step.callbacks:
                    self._moment(callback, "run")
            except CancelStepException:
                outcome = "cancelled"
            finally:
                for callback in step.callbacks:
                    self._moment(callback, "after")
        except CancelProcessException:
            outcome = "cancelled"
            raise
        except BaseException:
            outcome = "failed"
            raise
        finally:
            self._close(1, f"{step.name} {outcome}", started, outcome)

    def _moment(self, callback: Callback, moment: str) -> None:
        """Call one moment of a callback, with a line for its outcome when it does something."""
        method = getattr(callback, moment)
        # Judge the very object that is called, so inherited overrides and moments assigned on
        # the instance count; only `Callback`'s own do-nothing method gets no line.
        shown = getattr(method, "__func__", method) is not getattr(Callback, moment)
        started = time.perf_counter()
        try:
            method()
        except BaseException as exc:
            if shown:
                self._moment_failed(callback, moment, exc, time.perf_counter() - started)
            raise
        if shown:
            duration = _duration(time.perf_counter() - started)
            self._emit(logging.INFO, 2, "✔", "ok", f"{_name(callback)}.{moment} {duration}")

    def _moment_failed(
        self, callback: Callback, moment: str, exc: BaseException, seconds: float
    ) -> None:
        """Write the line of a moment that raised, by what the engine does with the exception."""
        who = f"{_name(callback)}.{moment}"
        duration = _duration(seconds)
        # The engine does not handle a CancelStepException raised from `after`.
        cancels_step = isinstance(exc, CancelStepException) and moment != "after"
        if cancels_step or isinstance(exc, CancelProcessException):
            scope = "step" if cancels_step else "process"
            message = _message(exc)
            reason = f": {message}" if message else ""
            text = f"{who} cancelled the {scope} after {duration}{reason}"
            self._emit(logging.INFO, 2, "⊘", "cancel", text)
            return
        message = _message(exc)
        detail = f"{type(exc).__name__}: {message}" if message else type(exc).__name__
        self._emit(logging.ERROR, 2, "✖", "fail", f"{who} failed after {duration}: {detail}")

    def _close(self, depth: int, text: str, started: float, outcome: str) -> None:
        """Write the closing line of a step or of the process."""
        duration = _duration(time.perf_counter() - started)
        level = logging.ERROR if outcome == "failed" else logging.INFO
        marker_outcome = {"failed": "fail", "cancelled": "cancel"}.get(outcome, "ok")
        self._emit(level, depth, "└", marker_outcome, f"{text} in {duration}")

    def _emit(self, level: int, depth: int, marker: str, outcome: str, text: str) -> None:
        """Write one line of the tree as one record, with the data the console handler colors."""
        line = TreeLine(f"[{self.run_id}] " + "│ " * depth, marker, text, outcome)
        self._log.log(level, "%s", line.plain(), extra={RECORD_KEY: line})


def _name(callback: Callback) -> str:
    """The callback's name, or its class name when `name` raises: a line never breaks a run."""
    try:
        return callback.name
    except Exception:
        return type(callback).__name__


def _message(exc: BaseException) -> str:
    """`str(exc)`, or an empty string when it raises: a line never replaces the exception."""
    try:
        return str(exc)
    except Exception:
        return ""


def _count(number: int, noun: str) -> str:
    """`1 step`, `0 steps`."""
    return f"{number} {noun}" if number == 1 else f"{number} {noun}s"


def _duration(seconds: float) -> str:
    """Format a duration, truncating every unit: `412ms`, `3.52s`, `2m 13s`, `1h 2m 3s`."""
    millis = int(seconds * 1000)
    if millis < 1_000:
        return f"{millis}ms"
    if millis < 60_000:
        return f"{millis // 1000}.{millis % 1000 // 10:02d}s"
    total = millis // 1000
    if millis < 3_600_000:
        return f"{total // 60}m {total % 60}s"
    return f"{total // 3600}h {total % 3600 // 60}m {total % 60}s"
