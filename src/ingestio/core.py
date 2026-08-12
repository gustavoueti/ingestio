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
        self._log.info("[%s] process started | %d steps", self.run_id, len(self.steps))

        try:
            for position, step in enumerate(self.steps, start=1):
                self._run_step(step, position)
        except CancelProcessException as exc:
            self.cancelled = True
            self._log.info("[%s] process cancelled: %s", self.run_id, exc)

        self._log.info("[%s] process finished | %.3fs", self.run_id, time.perf_counter() - started)
        return self

    def _run_step(self, step: Step, position: int) -> None:
        """Run the step in three passes: every `before`, every `run`, then every `after`.

        `after` runs whether the step completed, was cancelled, or the process is stopping.
        """
        started = time.perf_counter()
        self._log.info(
            "[%s] > %s (%d/%d) | %d callbacks",
            self.run_id,
            step.name,
            position,
            len(self.steps),
            len(step.callbacks),
        )
        try:
            for callback in step.callbacks:
                callback.before()
            for callback in step.callbacks:
                callback.run()
        except CancelStepException as exc:
            self._log.info("[%s] %s cancelled: %s", self.run_id, step.name, exc)
        finally:
            for callback in step.callbacks:
                callback.after()
            self._log.info(
                "[%s] < %s | %.3fs", self.run_id, step.name, time.perf_counter() - started
            )
