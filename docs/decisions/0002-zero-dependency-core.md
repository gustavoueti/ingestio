# 0002. Keep the core free of dependencies

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** `pyproject.toml` (`dependencies = []`), `src/ingestio/connector/http.py`; listed
  by the maintainer as a principle of the [philosophy](../philosophy.md#principles)

## Context

ingestio integrates with external systems, and most of them have client libraries of their
own.

## Decision

- `dependencies` stays empty.
- An integration that needs a third-party package declares it as an optional extra,
  `ingestio[<name>]`, added only through an approved spec.
- What the standard library can do, it does: `JsonApiReader` is built on `urllib`.

## Rationale

- "Engine dependencies, when they arrive, go in extras: the core stays installable
  anywhere." (`pyproject.toml`)
- `JsonApiReader` is "built on `urllib` so the package keeps no runtime dependencies."
  (`connector/http.py`)

## Consequences

- `JsonApiReader` does not support endpoints that require authentication.
- The pattern for extras is defined by
  [specs/postgres_connectors.md](../../specs/postgres_connectors.md), with the first extra,
  `ingestio[postgres]`: the extra is named after the system it integrates; third-party
  packages are imported only inside the functions that use them, so the package imports without
  any extra; a connector whose extra is missing raises `ImportError` naming it when it is
  built; an engine the runtime provides, such as pyspark, is a documented prerequisite and
  never part of an extra; the `dev` group holds the packages of every extra. See
  [Extras](../concepts/connectors.md#extras).
- The second extra is `ingestio[s3]`, with `boto3>=1.34`
  ([specs/s3_connectors.md](../../specs/s3_connectors.md)). It reads the third rule of the
  pattern as "when the connector needs the package": an `S3Reader` given a `client` imports no
  boto3, and an `S3Writer` never does, so only a reader without a `client` raises `ImportError`
  naming the extra.
- The third extra is `ingestio[gcs]`, with `google-cloud-storage>=2.10`
  ([specs/gcs_connectors.md](../../specs/gcs_connectors.md)). It follows the same reading of
  the third rule: only a `GcsReader` built without a `client` raises `ImportError` naming the
  extra, and a `GcsReader` given a `client` and a `GcsWriter` import no google-cloud-storage.
- The fourth extra is `ingestio[databricks]`, with `databricks-sdk>=0.20`
  ([specs/volume_connectors.md](../../specs/volume_connectors.md)). It follows the same reading
  of the third rule: only a `VolumeReader` built without a `client` raises `ImportError` naming
  the extra, and a `VolumeReader` given a `client` and a `VolumeWriter` import no
  databricks-sdk. The floor is the first version with the Files API's directory listing,
  `files.list_directory_contents`. Databricks Connect is never part of the extra: it replaces
  pyspark and is a prerequisite of jobs that run outside Databricks.
- The fifth extra is `ingestio[bigquery]`, with `google-cloud-bigquery>=3.20`
  ([specs/bigquery_connectors.md](../../specs/bigquery_connectors.md)). It follows the same
  reading of the third rule, applied to what each connector uses: a `BigQueryWriter` always
  imports it, for the staging table and the statements, and a `BigQueryReader` only for watermark
  reads, for the bound query, so a reader without `watermark_column` raises no `ImportError`. The
  extra asks for no extra of that package: the connectors need neither pandas nor pyarrow.
  spark-bigquery-connector is a JAR that the runtime provides, a prerequisite like pyspark, and
  never a Python dependency.
