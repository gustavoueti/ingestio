# 0017. Read a library callback's own parameters from a section named after it

- **Status:** accepted
- **Recorded:** 2026-10-24
- **Evidence:** [specs/scd2_callback.md](../../specs/scd2_callback.md), decisions 6 and 29, taken
  by the maintainer on 2026-10-23. It amends
  [ADR 0015](0015-choose-connectors-from-the-job-configuration.md).

## Context

- [ADR 0015](0015-choose-connectors-from-the-job-configuration.md) shapes the job's configuration
  file as one `source` and one `target` section, both holding a connector's parameters, and leaves
  the top level to the job: every other member is ignored by every callback
  ([specs/config_connectors.md](../../specs/config_connectors.md), decision 11).
- `SCD2` has parameters that belong to neither the connector nor the run: the key columns, the
  effective-time column, the tracked columns and the names of the version columns.
- The "Effects on the draft specs" of config_connectors left the choice between constructor
  arguments and members outside `source` and `target` to the spec of `SCD2`.

## Decision

- **`SCD2` reads its parameters from the top-level `scd2` section,** with the keys and rules of
  [the `scd2` section](../concepts/configuration.md#the-scd2-section), checked when the callback is
  built. They are not constructor arguments. No other callback reads the section, and `SCD2` reads
  no member other than `target` and `scd2`.
- **The section names no system and chooses no write strategy.** The configuration still has no
  mode, and the callback placed in the step chooses the strategy.
- **The top level stays the job's,** except for the names of the library's sections: `source`,
  `target` and `scd2`.
- **The general rule:** a library callback that has parameters of its own reads them from one
  top-level section named after its class in snake case, and no other callback reads it.

## Rationale

The maintainer's reason for putting SCD2's parameters in a section of the configuration file
rather than in constructor arguments (decision 6 of the spec): they are part of the job's
configuration, so they belong in its configuration file, which makes the callback more
customisable for whoever writes the job.

For the name and the shape of the section, the reasons are technical and come from decision 29 of
the spec:

- `scd2` is the callback's class name in snake case, the rule by which the `system` names come from
  the connector classes (config_connectors, decision 7).
- The keys end in `_column` or `_columns`, like `watermark_column`, the configuration's existing
  key for a column name. `key_columns` avoids `keys`, because the callback's `key` argument already
  names a key of the state.
- The general rule makes the top-level names that the library reserves a closed set derived from
  its callbacks, so a job knows which names its own members must avoid.

This is a new ADR, and not a decision of the spec alone, because ADR 0015 records the shape of the
file.

## Consequences

- **ADR 0015's shape of the file has a third section,** read by one callback. ADR 0015 is amended.
- **Decision 11 of config_connectors is superseded in part:** the top level of the file is the
  job's except for the `scd2` section, which `SCD2` reads. `ReadSource`, `AppendOnly` and
  `FullLoad` still ignore every member other than their own section, `scd2` included.
- **A job whose own top-level member is named `scd2` must rename it** before placing `SCD2`.
- **Changing the key columns or the tracked columns is an edit of the JSON.**
- **One dimension per file,** as for the other write callbacks.
- **Nothing closes the door to streaming** ([ADR 0012](0012-streaming-deferred.md)). The section
  holds parameters of a callback, and the `Reader` and `Writer` protocols and their payload do not
  change.
