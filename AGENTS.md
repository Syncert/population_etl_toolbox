# Repository Agent Instructions

## Purpose and architecture

This repository is the foundation for a public-data analytics website and social hub. It integrates public datasets into a trustworthy warehouse that will later support APIs, configurable analyses, visualizations, publishing, forums, and shared insights.

Implementation follows this dependency order:

```text
stable warehouse objects -> stable API contracts -> frontend analytics/social features
```

1. **Data warehouse — current priority.** Build trustworthy ingestion, raw capture, normalization, shared dimensions, quality controls, lineage, and publication-ready warehouse objects.
2. **API — second.** Build API resources only on stable warehouse contracts. Do not use the API to compensate for unfinished warehouse design.
3. **Web application/social hub — last.** Build frontend behavior against stable API contracts. Do not duplicate warehouse or API rules in client code.

When a downstream requirement exposes a missing upstream foundation, fix or plan the upstream contract first.

Preserve source fidelity, reproducibility, safe replay, explicit layer boundaries, provenance, privacy/security, and the distinction between provider-published facts and derived analysis.

Preserve unrelated user changes in the working tree.

## Canonical repository guidance

`AGENTS.md` contains repository-wide operating rules. Detailed contracts live under `docs/reference/`; do not duplicate them here.

Before changing behavior, read the applicable contract in full:

* `docs/reference/TESTING_CONTRACT.md` — test layers, markers, fixtures, isolation, infrastructure, quality gates, and behavioral catalog.
* `docs/reference/ADDING_A_DATA_SOURCE.md` — required adapter contract and checklist for public-data sources.
* `docs/reference/BETA_RESET_REINGESTION.md` — warehouse bootstrap, reset, dependency-order ingestion, replay, and re-ingestion.
* `docs/reference/CI_EVIDENCE_MAP.md` — automated evidence proving repository contracts.
* `docs/reference/API_CONSUMER_GUIDE.md` — the stable public API contract: routes, semantics, errors, caching, limits, and version policy.
* `docs/reference/PLAN_DISPATCHER.md` — parallel plan dispatch, worker prompts, verification, and integration.

When implementation intentionally changes a documented contract, update the applicable reference, implementation, and tests together.

If a plan conflicts with a reference contract, investigate repository evidence and resolve the conflict explicitly before implementing.

For architectural work, also inspect the relevant implementation, tests, and repository documentation. Read `README.md` or `pyproject.toml` when they are relevant to the decision; do not repeatedly reread them for routine plan execution.

## Plan workflow

Implementation plans live under `docs/plans/`:

* `to_do/` — approved, unclaimed work.
* `in_progress/` — currently active or interrupted work.
* `needs_review/` — implementation complete and awaiting human review.
* `completed/` — human-accepted work. Agents move plans here only when explicitly asked.

The folder containing a plan is its authoritative workflow state.

When the user asks to implement the backlog:

1. Inventory Markdown plans in `in_progress/` and `to_do/`. `docs/plans/README.md` is guidance, not a plan.
2. Resume `in_progress/` work first. If several plans exist, prefer dependency-unblocking work, then oldest by Git history, then filename order.
3. Otherwise choose an unblocked `to_do/` plan according to documented dependencies and `docs/plans/README.md`, move exactly one plan to `in_progress/`, and update its status/checkpoint.
4. Read the selected plan completely and the applicable reference contracts.
5. Inspect only the implementation and tests needed to establish the current gap.
6. Implement and validate the plan incrementally.
7. Keep the active plan current with verified progress, decisions, evidence, remaining work, and blockers.
8. When the definition of done is satisfied, record concise implementation/validation evidence, mark it ready for review, move it to `needs_review/`, and repair affected plan links.
9. Re-inventory `in_progress/` and `to_do/` before selecting the next plan.

When the user requested the whole backlog, completion of one plan is a checkpoint, not a stopping condition.

The loop ends only when both active folders contain no Markdown plans and all handled plans have reached `needs_review/`, unless a genuine blocker prevents progress.

## Implementation discipline

Favor implementation and executable evidence over prolonged repository exploration.

For each problem:

1. Read the active plan and applicable contract.
2. Locate the relevant implementation and tests.
3. Establish the failing or incomplete behavior.
4. Form the smallest evidence-supported hypothesis.
5. Write or update the smallest deterministic test that expresses the required behavior and important failure path.
6. For new behavior, confirm the test fails for the expected reason.
7. Implement the smallest coherent change.
8. Run focused validation immediately.
9. Run the affected component/contract suite after focused validation passes.
10. Inspect `git diff` after material edits.

Do not continue implementation past a failing focused or affected suite.

Tests are executable evidence, not automatically the product specification. Resolve disagreements using the active plan and repository contracts. Fix stale tests when the documented contract proves them wrong; otherwise fix the implementation.

Do not weaken assertions, accumulate known failures for later cleanup, or treat skipped/deselected/unexecuted tests as passing evidence.

## Tool and context discipline

The repository, plans, Git diff, and tests are persistent project memory. Conversation history is temporary working context.

Use the shortest evidence-gathering path sufficient to act:

* Prefer `rg`, symbol searches, filename filters, and targeted directories over broad recursive dumps.
* Read bounded file sections when the whole file is unnecessary.
* Prefer focused tests while iterating.
* Capture relevant failures and tracebacks rather than repeatedly printing complete logs.
* Do not reread an unchanged file unless a new question requires it.
* Do not dump generated artifacts, datasets, lock files, fixtures, or large logs for orientation.
* Summarize evidence already established instead of retrieving it again.

### Anti-loop rule

Do not repeat a read, search, diagnostic, or tool action unless repository state changed or a specific new question requires it.

If roughly **10–15 diagnostic/read actions** occur on one issue without an implementation change or materially new evidence, stop exploring and do one of:

* make the smallest justified change and test it;
* run one targeted experiment that distinguishes the remaining hypotheses; or
* record a concrete blocker.

If the information needed to answer or act has already been found, stop exploring and proceed.

After context condensation or session restart, recover state from the active plan, Git diff, tests, and repository files rather than reconstructing the previous conversation.

### Conflicting filesystem evidence

If tools disagree about file contents:

1. Confirm they reference the same absolute path.
2. Read the file through the filesystem/runtime used by the application or tests.
3. If useful, compare size, modification time, or hash.
4. Run the relevant executable test/import against that filesystem.
5. Treat the representation consumed by runtime/tests as authoritative.

Once a viewer is proven stale, stop using it as the authoritative reader for that session unless new evidence requires rechecking.

## Definition of done

A plan may move to `needs_review/` only when:

* all acceptance criteria and in-scope deliverables have inspectable repository evidence;
* required tests were added or updated and pass, including meaningful failure paths and contract boundaries where applicable;
* relevant formatting, lint, build, schema, migration, and documentation checks pass;
* applicable requirements in `docs/reference/` remain synchronized with implementation;
* affected documentation, configuration, fixtures, migrations, manifests, and operational instructions are current;
* no unresolved in-scope TODO, placeholder, skipped requirement, or known defect remains;
* secrets and credentials are absent from code, fixtures, logs, request metadata, and committed files;
* no acceptance criterion or scope has been silently weakened; and
* validation commands/results and environment-limited checks are recorded in the plan.

If a required check cannot run, record the exact blocker. Do not report it as passing.

If completion requires a user decision, credential, unavailable external system, or unmet dependency, keep the plan in `in_progress/`, record the blocker and completed evidence, and continue only with genuinely independent work.

## Validation

Use the smallest relevant checks while iterating and the broadest practical applicable checks before review.

* Python: `pytest`
* Lint: `ruff check .`
* Makefile targets where applicable: `test-unit`, `test-etl`, `test-api`, `test-dags`, and relevant integration/e2e targets.
* Web unit tests: `npm --prefix apps/web run test:unit`
* Web lint: `npm --prefix apps/web run lint`
* Web build: `npm --prefix apps/web run build`

Integration, database, Redis, Martin, external-source, performance, browser, and composed-service checks may require optional infrastructure. Run them when the changed contract depends on them and record unavailable environments explicitly.

## Engineering invariants

* Preserve raw source evidence before parsing or normalization.
* Keep raw capture, control state, silver normalization, gold publication, and serving/API responsibilities explicit.
* Prefer deterministic offline fixtures for provider behavior; live calls supplement rather than replace replayable tests.
* Preserve idempotency, revision history, geography identity, units, suppression/missing semantics, and lineage.
* Use authoritative geography codes/mappings; do not infer identity from names when authoritative identifiers are required.
* Never silently convert suppressed, missing, invalid, or non-numeric values to zero.
* Keep provider adapters isolated and shared contracts provider-neutral.
* Avoid unrelated refactors.
* Prefer small, reviewable changes with immediate validation.
* Never delete or overwrite unrelated user work.
* Do not commit, push, deploy, rotate credentials, or modify external systems unless explicitly requested.

## Terminal safety

For long-running commands:

1. Use finite timeouts for tests, normally 120 seconds.
2. A terminal soft timeout does not by itself mean the command failed.
3. Poll an active command once.
4. If still stuck, send `Ctrl+C` **once**.
5. Poll once after the interrupt.
6. If the terminal remains unavailable, reset the terminal session.

Never send repeated `Ctrl+C` input or repeat an identical terminal action that failed to change state.

For diagnosing a hanging test, prefer:

```text
python -u -m pytest <path> -vv -s --tb=short --maxfail=1
```

Do not pipe diagnostic pytest output through PowerShell filtering. If a suite hangs, isolate the last-running test instead of repeatedly rerunning the entire suite.

## Handoff and stopping conditions

At meaningful checkpoints, persist enough state in the active plan for another agent to resume from repository evidence.

User-facing progress reports should state:

* the active or newly completed plan;
* implemented behavior and validation evidence;
* remaining active plans; and
* blockers or checks not run.

When the whole backlog was requested, do not stop merely to provide a progress report if safe actionable work remains.

Stop autonomous execution only when:

1. the requested backlog is complete;
2. a genuine blocker requires user input, credentials, unavailable infrastructure, or a scope decision;
3. continuing risks unrelated user work or violates a repository contract; or
4. the environment can no longer reliably read, modify, or validate the repository.

### Windows terminal rules

The execution environment is Windows PowerShell unless direct evidence shows otherwise.

- Use PowerShell-native syntax and commands.
- Do not use Bash operators or utilities such as:
  - `&&`
  - `grep`
  - `find`
  - `cat`
  - `head`
  - `tail`
  - `/dev/null`
  - Bash-style environment-variable assignment
- Use PowerShell equivalents:
  - command sequencing: `;`
  - search text: `Select-String`
  - recursive file discovery: `Get-ChildItem -Recurse`
  - read files: `Get-Content`
  - change directory: `Set-Location`
  - suppress output/errors only with PowerShell syntax
- Prefer running repository Python commands from an explicit working directory rather than embedding shell-specific directory changes when avoidable.
- Do not repeat an identical terminal command after it hangs or fails unless repository state changed or a specific new hypothesis justifies the rerun.

When a terminal command fails or times out:

1. Poll the existing command once.
2. If it is still running, send `Ctrl+C` exactly once.
3. Poll once after the interrupt.
4. If the process is still running or the terminal remains unavailable, reset the terminal session immediately using the terminal tool's reset capability.
5. After reset, run a trivial PowerShell health check before executing another repository command.
6. Do not diagnose Python, pytest, PATH, repository state, or dependency availability until the fresh terminal successfully executes the health check.

**Hard process-management invariant:** Never send `Ctrl+C` more than once for the same running process. After one unsuccessful interrupt and one poll, the only permitted terminal recovery action is a terminal reset.

### Terminal timeout invariants

- Terminal tool `timeout` values are measured in **seconds**, never milliseconds.
- Never set a terminal timeout above **300 seconds** unless the active plan explicitly requires a known long-running operation.
- Normal focused test timeout: **120 seconds**.
- Normal larger-suite timeout: **300 seconds maximum**, unless explicitly justified.
- Normal non-test command timeout: **30–120 seconds**.
- Never use values such as `60000`, `120000`, or other millisecond-style timeout values.
- A terminal soft timeout means the process may still be running; it does not prove the command or test failed.

### Test command invariants

For diagnostic pytest runs, execute pytest directly.

Preferred:

`python -u -m pytest <target> -vv -s --tb=short --maxfail=1`

Do not pipe diagnostic pytest output through `Select-String`, `Select-Object`, `grep`, `head`, `tail`, or equivalent filtering commands.

If pytest exceeds its terminal timeout:

1. Poll once.
2. Send `Ctrl+C` once if the process is still active.
3. Poll once.
4. If it remains active or the terminal remains unavailable, reset the terminal session.
5. Verify the fresh terminal with a trivial command.
6. Diagnose the individual hanging test before running another suite.
7. Do not repeatedly rerun the same hanging test without gathering new diagnostic evidence.

If a direct Python import or test consistently hangs, prefer a bounded diagnostic that produces actionable evidence rather than another blind rerun. For example, use Python `faulthandler` or another finite diagnostic mechanism to capture where execution is blocked.

### Subagent delegation

On Windows, do not delegate terminal execution, test execution, process management, or shell diagnosis to `bash-runner`.

The primary agent is responsible for terminal recovery and diagnosis of hanging or failed test commands.

If terminal work is delegated to another subagent:

- explicitly include the Windows PowerShell rules from this repository;
- explicitly state that terminal timeout values are in seconds;
- explicitly state the one-`Ctrl+C` hard limit and required terminal-reset behavior;
- require direct pytest execution without output-filtering pipelines; and
- require the subagent to return concrete results rather than silently completing without evidence.

The parent agent remains responsible for ensuring all delegated commands obey repository terminal-safety constraints.

### External research rules

Internet research is permitted and should be used when implementation depends
on information not reliably established by the repository.

Use authoritative sources before guessing about:

- API endpoints, schemas, parameters, authentication, and rate limits
- current library/framework behavior
- external dataset definitions and geographic coverage
- compatibility or version-specific behavior
- unfamiliar errors that cannot be resolved from repository context

Prefer primary sources in this order:

1. Official documentation
2. Official source repositories / release notes
3. Standards or government documentation
4. Reputable technical references

Do not repeatedly search the web when the repository or an authoritative
source already answers the question.

After research, return to implementation. Web research is supporting work,
not a substitute for modifying and testing the repository.