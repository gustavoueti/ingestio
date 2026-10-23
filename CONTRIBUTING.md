# Contributing to ingestio

This is the prescriptive reference for code, tests, commits and pull requests. How a change
moves from request to merge is described in [docs/workflow.md](docs/workflow.md); the design
rules every change must respect are in [docs/philosophy.md](docs/philosophy.md), which wins
over this file when the two disagree.

## Setup

```bash
uv sync                                             # environment, with the dev group
uv run pre-commit install --hook-type commit-msg    # once per clone
```

The `commit-msg` hook rejects commit messages that carry a Claude `Co-Authored-By` trailer or
a "Generated with Claude Code" footer.

## Checks

The same commands CI runs, on Python 3.11, 3.12 and 3.13:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q
```

CI installs with `uv sync --locked`, so a `uv.lock` that is out of date fails the build.
Commit `uv.lock` together with any change to dependencies.

## Code

### Layout

- `src/ingestio/<subpackage>/core.py` holds the abstractions of a subpackage, such as
  `Callback` or the `Reader` and `Writer` protocols.
- Implementations are named after the medium they work with: `connector/http.py`,
  `connector/local.py`, `callback/io.py`.
- `<subpackage>/all.py` is the import surface of a subpackage. The subpackage `__init__.py`
  holds only a docstring.
- `ingestio/basics.py` gathers the core, and `ingestio/__init__.py` re-exports it with
  `__version__`.
- Every module has a docstring and an explicit, sorted `__all__`.

### Style

- Modules with logic start with `from __future__ import annotations`.
- Docstrings follow the Google convention. A class documents its constructor arguments in its
  own docstring, under `Args:`.
- Comments explain why, not what.
- Required arguments are positional; options are keyword-only, after `*`.
- Validate arguments when the object is built and raise `ValueError` with a lowercase message
  and no trailing period, such as `"a process needs at least one step"`.
- A connector defines a `__repr__` that shows what identifies it:
  `JsonApiReader(url='https://...')`.
- Prefer immutable structures: tuples, frozen dataclasses.
- Datetimes are timezone-aware (`datetime.now(UTC)`). Paths use `pathlib`.
- Lines are at most 100 characters; `ruff format` decides the rest.

### Types

- Everything is annotated. mypy runs in strict mode over `src` and `tests`.
- Payloads are `Any`: the library does not constrain what flows between stages.
- Generic protocols use variance-suffixed type variables, `T_co` and `T_contra`.
- The package ships `py.typed`.

### Logging

- Use the standard `logging` module, never `print`: `logger = logging.getLogger(__name__)`.
- Pass arguments `%`-style, so formatting only happens when the record is emitted.
- Start every line with the run id, `[a3f7c9] ┌ process started · 2 steps`, including each
  line of a message that spans several.
- Messages never carry ANSI codes: color is added on output, only by the handler that
  `log_to_console` attaches.
- The package root registers a `NullHandler`; configuring logging is the application's job.
  `log_to_console` is the opt-in shortcut, and it touches only the `ingestio` logger.

### Dependencies

- The core has none: `dependencies = []` stays empty.
- An integration that needs a third-party package declares it as an extra,
  `ingestio[<name>]`, and only through an approved spec.
- Development tools go into the `dev` dependency group.

### Public API

The public API is everything exported by `ingestio`, `ingestio.callback.all` and
`ingestio.connector.all`, their import paths, and the documented attributes
(`Process.state`, `Process.run_id`, `Process.cancelled`, `Process.started_at`,
`Callback.process`). Any change to it needs an approved spec.

## Tests

- Tests are plain pytest functions, without test classes, one file per area, each with a
  module docstring.
- A test name is a sentence about behaviour:
  `test_cancel_step_skips_the_rest_of_the_step_but_continues_the_process`.
- A test that covers an acceptance criterion of a spec is named after it,
  `test_at001_<description>`, and lives in the spec's file, `tests/test_<slug>.py`.
- Tests are fully typed.
- Test doubles live in the test file that uses them, with a leading underscore
  (`_StubReader`). Prefer a real local resource over a mock: the `api_url` fixture in
  `tests/conftest.py` serves JSON from a local HTTP server.
- Use `tmp_path` for files, `caplog` for logs, and `pytest.raises(..., match=...)` for errors.
- Mark tests that touch an external resource (network, heavy disk, an engine) with
  `@pytest.mark.integration`.

### Documentation examples

Every `python` block in `docs/` runs as written: `tests/test_docs_examples.py` executes each
one in a temporary directory. Keep examples self-contained. A block that cannot run on its own,
because it calls a network service for instance, carries `<!-- example: skip -->` on the line
right above its opening fence.

## Commits and pull requests

- Commit messages and pull request titles follow
  [Conventional Commits](https://www.conventionalcommits.org/): `type: subject`, with a
  lowercase subject in the imperative mood and no trailing period. Accepted types: `feat`,
  `fix`, `perf`, `refactor`, `docs`, `test`, `build`, `ci`, `chore`, `revert`.
- Pull requests are squash-merged, so the PR title becomes the commit message on `main`, and
  python-semantic-release derives the next version from it. CI checks the title before the
  merge.
- Branches are named `<type>/<author>/<topic>`, such as `ci/ueti/fix_release_checkout`.
- No `Co-Authored-By` trailers for AI tools and no "Generated with" footers, in commits or in
  pull request descriptions.
- Releases, tags and PyPI publishing are automated on merge to `main` and belong to the
  maintainer.
