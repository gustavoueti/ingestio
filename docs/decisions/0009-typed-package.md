# 0009. Ship a typed package and check it with mypy in strict mode

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** `src/ingestio/py.typed`, `pyproject.toml` (`[tool.mypy] strict = true`, the
  `Typing :: Typed` classifier), `.github/workflows/ci.yml`

## Context

The library exposes protocols that only a type checker enforces
([ADR 0003](0003-connectors-as-protocols.md)).

## Decision

- The package ships `py.typed` (PEP 561), so type checkers use its annotations.
- mypy runs in strict mode over `src` and `tests`, in CI, on every supported Python version.

## Rationale

> TODO(maintainer): why the package is typed and why strict mode covers the tests too.

## Consequences

- Every function, test included, is fully annotated.
- The protocols of [ADR 0003](0003-connectors-as-protocols.md) are checked by the user's type
  checker, not at runtime.
