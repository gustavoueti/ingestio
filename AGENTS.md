# AGENTS.md

Instructions for AI coding agents working on ingestio. This file is short on purpose: it
points to the detail instead of repeating it. Start every task at
[docs/index.md](docs/index.md).

## Philosophy in brief

The canonical text is [docs/philosophy.md](docs/philosophy.md). In short:

- **Two concepts.** A connector talks to an external system through the `Reader` and/or
  `Writer` protocols and decides nothing else. The library ships every connector, and a job
  names its source and target in its configuration. A callback is a stage of the pipeline
  that performs an action. Something that fits neither: stop and report it.
- **Essential by default.** A failing essential callback fails the pipeline. A non-essential
  one is an explicit choice of its author, and its failure is recorded in the run result,
  never swallowed.
- **Simplicity.** The smallest abstraction that works: no internal frameworks, no open
  registries, no magic. The one mapping from names to classes is the closed table of shipped
  connectors ([ADR 0015](docs/decisions/0015-choose-connectors-from-the-job-configuration.md)).
- **Compose, do not impose.** Extending the library stays as simple as subclassing `Callback`
  in your own code.
- **No dependencies in the core.** Integrations are optional extras.
- **Streaming is future work.** A decision that closes the door to it needs an ADR.

## Hierarchy of truth

1. [docs/philosophy.md](docs/philosophy.md)
2. [CONTRIBUTING.md](CONTRIBUTING.md) for code, tests, commits and pull requests;
   [docs/workflow.md](docs/workflow.md) for the development flow
3. AGENTS.md
4. `.claude/kb/`

When two sources disagree, the higher one wins and the lower one must be fixed. A spec that
contradicts the philosophy is wrong, unless it comes with an ADR that changes the philosophy.

## Ask the maintainer

When a task needs a definition that only the maintainer can give, ask for it before going
further. Never fill the gap with a guess, a convenient default or the most likely answer. This
covers anything the philosophy, the docs, the spec and the code do not settle: behaviour, edge
cases, naming, scope, a trade-off between principles, the intent behind a request.

- Ask as early as possible, before writing anything that depends on the answer.
- Ask everything at once, each question with the options you see and the one you recommend.
- An agent of the development flow cannot reach the maintainer: it stops and reports the
  question, and the main session takes it to the maintainer. In planning, the questions go
  under **Open questions** in the spec.

## Reasons for a decision

Only technical reasons justify a concept, a decision or the rationale of an ADR: what the
philosophy, an ADR, the code or a measurement establishes, or a technical specification the
maintainer gives for the decision. A personal opinion never justifies anything in this
project, and neither does a remark made in passing, the maintainer's included: it is context
for the conversation, not a reason to record.

- Tell apart a conversation that defines a direction, with technical specifications, from what
  is said around it.
- Record in a spec or an ADR only the technical reasons given for a decision. When none is
  given, write `TODO(maintainer)`, never a reason built from a remark or an opinion.
- When relaying the maintainer's answers to an agent, pass the decision and its technical
  reasons, not the remarks around them.

## Constraints

- **Never write to git or GitHub.** Reading is fine: `git log`, `show`, `diff`, `status`,
  `blame`, `shortlog`, `ls-files`, `rev-parse`, `branch` to list, and the read commands of
  `gh` such as `gh pr view`, `gh pr list` or `gh run view`. Anything that changes the index,
  refs, history or a remote is the maintainer's:
  - in git: add, commit, push, merge, rebase, reset, revert, cherry-pick, tag, stash,
    checkout, switch, restore, creating or deleting branches, worktrees;
  - in gh: creating, editing, merging or closing pull requests, creating releases, and any
    `gh api` call other than GET.
- **Never use worktree isolation.** A worktree creates a branch.
- **Never add `Co-Authored-By` trailers or AI-generation footers to commits or PRs,**
  regardless of the tool used.
- **Never publish to PyPI or create a release or tag.**
- **Never add a dependency to the core.** A new dependency goes into an extra, and only with
  an approved spec.
- **Any change to the public API needs an approved spec.** The public API is everything
  exported by `ingestio`, `ingestio.callback.all` and `ingestio.connector.all`, their import
  paths, and the documented attributes: `Process.state`, `Process.run_id`,
  `Process.cancelled`, `Process.started_at` and `Callback.process`.

## Finishing a task

End with a summary of the changes and a suggested commit message in Conventional Commits
form, with no co-author trailers. The maintainer runs the commit.

## Development workflow

Every change to `src/` goes through five stages, each with its own agent:

planning → tests → documentation → implementation → validation

The spec in `specs/` records where the change stands. Statuses, rules and return paths are in
[docs/workflow.md](docs/workflow.md).

## Documentation map

[docs/index.md](docs/index.md) maps every document: philosophy, workflow, concepts, decisions
and specs.
