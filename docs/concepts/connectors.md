# Connectors

A connector is an integration with an external system. It knows how to talk to that system
and nothing else: it does not decide write strategy, format or architecture. Those belong to
[callbacks](callbacks.md). See the [philosophy](../philosophy.md#connector).

## Capabilities

A connector declares what it can do through two capabilities, defined as protocols in
`ingestio.connector.core`:

```python
from typing import Protocol, TypeVar

T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


class Reader(Protocol[T_co]):
    def read(self) -> T_co: ...


class Writer(Protocol[T_contra]):
    def write(self, data: T_contra) -> None: ...
```

- A connector that can read has a `read()` method; one that can write has `write(data)`; one
  that can do both has both.
- Protocols are structural: a class is a connector because it has the method, not because it
  inherits from anything. There is no base class and no registry.
- The payload type is unconstrained. A reader may return a list of records, a generator, a
  DataFrame or bytes.

`Reader` and `Writer` are exported by `ingestio` and by `ingestio.connector.all`, for type
annotations.

## Using a connector

A connector is injected into the callback that needs it, so a single callback may combine
several. The library ships two callbacks that move data between a connector and the
[shared state](callbacks.md#sharing-data): `ReadSource` and `WriteTarget`.

```python
from pathlib import Path
from typing import Any

from ingestio import Process, ReadSource, Step, WriteTarget
from ingestio.connector.all import JsonLinesWriter


class StaticReader:
    """A connector that reads records held in memory."""

    def __init__(self, records: list[dict[str, Any]]) -> None:
        self.records = records

    def read(self) -> list[dict[str, Any]]:
        return self.records


Process([
    Step("extract", [ReadSource(StaticReader([{"id": 1}, {"id": 2}]))]),
    Step("load", [WriteTarget(JsonLinesWriter("volume/orders.jsonl"))]),
]).run()

print(Path("volume/orders.jsonl").read_text(encoding="utf-8"))
```

`StaticReader` is a complete connector: it satisfies `Reader[Any]` for the type checker
without importing anything from ingestio.

## Connectors shipped with the library

### `JsonApiReader`

`ingestio.connector.all.JsonApiReader(url, *, timeout=30.0, headers=None, encoding="utf-8")`

Fetches a JSON endpoint and returns the decoded payload.

- Built on `urllib`, so it adds no dependency.
- Accepts only `http` and `https` URLs; any other scheme raises `ValueError` when the reader
  is built.
- Sends `Accept: application/json`, plus the `headers` given.
- Decodes the body with the charset the response declares, or `encoding` when it declares
  none.
- HTTP errors are not swallowed: they propagate as `urllib.error.HTTPError`.
- Endpoints that require authentication are not supported.

<!-- example: skip -->
```python
from ingestio.connector.all import JsonApiReader

orders = JsonApiReader("https://api.example.com/v1/orders", timeout=10).read()
```

### `JsonLinesWriter`

`ingestio.connector.all.JsonLinesWriter(path, *, append=False, encoding="utf-8")`

Writes records as newline-delimited JSON, one object per line.

- Creates missing parent directories.
- Replaces the file on each write, or appends to it with `append=True`.
- Serialises values JSON does not know, such as datetimes, with `str`.
- A Databricks volume is an ordinary path, so the same writer serves a local disk and a
  cluster.

## Known divergence

`JsonLinesWriter` decides two things the philosophy assigns to callbacks: the write strategy
(`append`) and the format (JSON Lines). Moving them out changes the public API, so it waits
for a spec. See [specs/append_only_callback.md](../../specs/append_only_callback.md).

## Extras

No connector needs a third-party package yet. When one does, its dependency becomes an
optional extra, `ingestio[<name>]`, and the core stays without dependencies
([ADR 0002](../decisions/0002-zero-dependency-core.md)).
