# 0010. Release from the commit history on main

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `pyproject.toml`
  (`[tool.semantic_release]`)

## Context

Every merge to `main` may or may not be worth a release, and the version number has to follow
from what changed.

## Decision

- Pull requests are squash-merged, and their titles follow Conventional Commits; CI checks the
  title.
- On every push to `main`, python-semantic-release derives the next version from the commit
  types. When a release is due, it tags the version, builds with `uv build`, and the workflow
  publishes to PyPI through trusted publishing.
- Versions stay below 1.0.0 for now: `allow_zero_version = true`, `major_on_zero = false`.
- The release job checks out the branch, not the commit SHA.

## Rationale

- The PR title check: "Merges are squashed, so the PR title becomes the commit message on main
  -- and that message is what decides the next version. A wrong title means a wrong release,
  so it is checked before the merge, not after." (`ci.yml`)
- The zero versions: "Both differ from the defaults: without them the first automated release
  would jump straight to 1.0.0." (`pyproject.toml`)
- The checkout: "checking out a SHA gives a detached HEAD, and semantic-release needs to be on
  a branch to match its release groups." (`release.yml`)

> TODO(maintainer): why python-semantic-release, and why trusted publishing.

## Consequences

- Releases, tags and publishing need no manual step, and belong to the maintainer: no
  contributor or agent creates them.
- A commit type decides a release, so the type of a PR title matters as much as its code.
