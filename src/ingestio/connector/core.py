"""`Reader` and `Writer`: the two kinds of connector.

Every connector the library ships satisfies one of these protocols, and the read and write
callbacks build the connector that the job's configuration names. The payload type is
unconstrained.
"""

from typing import Protocol, TypeVar

__all__ = ["Reader", "Writer"]

T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


class Reader(Protocol[T_co]):
    """Reads data from a source."""

    def read(self) -> T_co: ...


class Writer(Protocol[T_contra]):
    """Writes data to a destination."""

    def write(self, data: T_contra) -> None: ...
