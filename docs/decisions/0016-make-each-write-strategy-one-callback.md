# 0016. Make each write strategy one callback

- **Status:** accepted
- **Recorded:** 2026-10-23
- **Evidence:** [specs/append_only_callback.md](../../specs/append_only_callback.md), decisions 1
  to 14, taken by the maintainer on 2026-10-23. It amends
  [ADR 0015](0015-choose-connectors-from-the-job-configuration.md).

## Context

- `WriteTarget(config, *, key="records", replace=False, ...)` chose between appending and
  replacing with its `replace` argument: one callback held two write strategies.
- `JsonLinesWriter(path, *, append=False)` fixed the strategy when the writer was built, the
  opposite of every other shipped writer, whose `write(data, *, replace=False)` leaves the choice
  to its caller. That put a write strategy in a connector, against the
  [philosophy](../philosophy.md#connector), which says a connector decides no write strategy.
  `WriteTarget` hid it by building the writer with `append=not replace` and branching on the
  writer's type when it wrote.
- SCD2 was planned as a write strategy ([specs/scd2_callback.md](../../specs/scd2_callback.md)).
  It has since shipped as `SCD2`, one more callback built as this decision says.

## Decision

- **Each write strategy is one callback.** `AppendOnly(config, *, key="records", connection=None,
  client=None)` adds a value of the shared state to the target that the `target` section of the
  job's configuration names. `FullLoad(config, ...)`, with the same signature, makes that target
  hold exactly the value. Both build their writer from the configuration as ADR 0015 describes,
  through the same closed table, and accept no connector. `WriteTarget` is removed from the public
  API, plainly: importing it raises Python's `ImportError`.
- **No connector decides the write strategy.** `JsonLinesWriter` loses `append` and gets
  `write(data, *, replace=False)`, like every other shipped writer. `AppendOnly` calls
  `write(data)` and `FullLoad` calls `write(data, replace=True)`, for every system, with no branch
  on the writer's type.
- **A write strategy the library adds later** (`SCD2`, since shipped, or a merge) is one more
  callback that builds its writer from the `target` section. The configuration has no mode.
- **An empty batch always calls the writer.** The callbacks hand `state[key]` to the writer as it
  is, never copied, converted, iterated, counted or type-checked, and leave the state unchanged.
  What an empty write does to the target is the connector's
  ([ADR 0014](0014-create-a-missing-target-only-when-derived-from-the-data.md)).
- **Both are essential**, as ADR 0011 requires of append-only: they declare no criticality and
  catch nothing.

## Rationale

The maintainer's reason for the separate callbacks: each write strategy is one callback, as SCD2
is.

For removing `append` from `JsonLinesWriter`, the purpose the maintainer stated is that no
connector decides the write strategy any more, which is the philosophy's sentence on connectors.

For never inspecting the batch, the maintainer's reason is that checking a DataFrame runs a Spark
job and checking a generator consumes it.

For removing `WriteTarget` without a deprecation, the maintainer's reason is that the library is
still in development, so its API can change.

> TODO(maintainer): why the names `AppendOnly` and `FullLoad`.

> TODO(maintainer): why the philosophy stays unchanged, although `FullLoad` is a third shipped
> write strategy beside the "append-only and SCD2" it names as the default callbacks.

> TODO(maintainer): why the JSON Lines format stays a known divergence of `JsonLinesWriter`, out
> of this decision.

> TODO(maintainer): why a failed `json_lines` write stays non-atomic, leaving what was written
> before the failure.

> TODO(maintainer): why the documentation carries no migration table from `WriteTarget` to
> `AppendOnly` and `FullLoad`.

## Consequences

- **The change breaks the public API.** `WriteTarget` and the `append` argument of
  `JsonLinesWriter` are gone; the package is alpha.
- **ADR 0015 is amended.** Where it names `WriteTarget`, the library's write callbacks are
  `AppendOnly` and `FullLoad`: each reads the `target` section. The rest of ADR 0015 stands,
  among it the closed table and the rule that no connector instance is accepted.
- **`JsonLinesWriter(path).write(data)` now appends,** where it replaced the file. A job's own
  callback that relied on the old default writes more lines than before, and nothing fails.
- **`FullLoad` over an empty batch empties the target.** A job that keeps its target when a batch
  is empty places its own callback before the write.
- **An append is not idempotent:** running the same batch twice appends it twice.
- **A failed `json_lines` write leaves what was written before the failure.** After a failed
  `FullLoad`, the file holds only that part.
- **The "Known divergence" of the connectors page narrows** to the format of `JsonLinesWriter`.
- **One target per configuration file** still holds: a job that appends to one target and
  replaces another uses two files, one per callback.
- **Nothing closes the door to streaming** ([ADR 0012](0012-streaming-deferred.md)). The
  callbacks hand the value to the writer as it is, so a generator in the state reaches
  `JsonLinesWriter` unconsumed and is written record by record.
