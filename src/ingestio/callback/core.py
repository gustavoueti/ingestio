"""`Callback` base class and the cancel exceptions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ingestio.core import Process

__all__ = ["Callback", "CancelProcessException", "CancelStepException"]


class CancelStepException(Exception):
    """Skip the remaining callbacks of the current step and move on to the next step."""


class CancelProcessException(Exception):
    """Stop the process cleanly, without treating the run as a crash."""


class Callback:
    """One stage of a process.

    A step runs its callbacks in three passes: every `before`, then every `run`, then every
    `after`. `after` runs even when the step or the process was cancelled.

    Callbacks exchange data through `self.process.state` rather than through arguments and
    return values.
    """

    #: Set by `Process` when the callback is attached. Unbound before that.
    process: Process

    def before(self) -> None:
        """Run before any callback of this step does its work."""

    def run(self) -> None:
        """Do the work of this stage."""

    def after(self) -> None:
        """Run after every callback of this step, including when the step was cancelled."""

    @property
    def name(self) -> str:
        """Display name used in logs."""
        return type(self).__name__

    def __repr__(self) -> str:
        return self.name
