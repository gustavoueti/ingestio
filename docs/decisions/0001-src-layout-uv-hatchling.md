# 0001. Package with a src layout, hatchling and uv

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** `pyproject.toml`, `uv.lock`, `.gitignore`, `.github/workflows/ci.yml`

## Context

The package needs a build backend, a way to manage environments and a reproducible set of
development dependencies.

## Decision

- The package lives under `src/ingestio/` (src layout).
- hatchling is the build backend; it reads the version from `src/ingestio/__init__.py`.
- uv manages the environment. `uv.lock` is committed, and CI installs with
  `uv sync --locked`.
- Development tools are a PEP 735 dependency group, `dev`, which is not published with the
  package.

## Rationale

- Committing the lock: "uv.lock is deliberately NOT ignored: it is committed for reproducible
  builds." (`.gitignore`)
- `--locked` in CI: it "fails when uv.lock is out of date, instead of silently resolving
  something different from what a developer has locally." (`ci.yml`)
- Development group: "Not published with the package and not installable as consumer-facing
  extras." (`pyproject.toml`)

> TODO(maintainer): why the src layout, why hatchling, and why uv.

## Consequences

- Tests run against the installed package, not against the source tree on `sys.path`.
- A change to dependencies must update `uv.lock` in the same commit, or CI fails.
