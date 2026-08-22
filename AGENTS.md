# Repository agent instructions

## Purpose

This repository is the foundation for a public-data analytics website and social hub. The finished product should allow users to explore public datasets integrated into the warehouse, configure and save their own analyses, compare measures across sources and geographies, and communicate what they learn through blog posts, forums, and shared insights.

The product is delivered through a strict implementation hierarchy. Each layer depends on a stable, validated foundation from the layer before it:

1. **Data warehouse — first:** Build trustworthy ingestion, raw capture, normalization, shared dimensions, quality controls, lineage, and publication-ready warehouse objects for supported public sources. This is the current phase and the present implementation priority. A warehouse object must have a stable grain, identity, semantics, provenance, quality contract, and test evidence before downstream work relies on it.
2. **API — second:** Build API resources from the respective stable warehouse objects. Each API contract must deliberately map to validated warehouse data products rather than compensate for unfinished schemas, duplicate warehouse logic, or query unstable internals. The API should support discovery, filtering, comparison, analysis configuration, persistence, and future social features. Begin an API surface only after its required warehouse objects are implementation-complete or explicitly approved as stable dependencies.
3. **Web application and social hub — last:** Build frontend features against stable, documented API contracts. The website must consume the API rather than couple directly to warehouse tables or recreate API and warehouse rules in client code. Analytics configuration and visualization come after their supporting API contracts; blogging, forums, publishing, and insight-sharing workflows build on those stable analytics capabilities.

This order is an architectural dependency, not merely a scheduling preference:

```text
stable warehouse objects -> stable API contracts -> frontend analytics and social features
```

Do not implement a downstream layer as a workaround for missing or unstable behavior upstream. When downstream requirements reveal a missing foundation, update or create the appropriate upstream plan and stabilize that layer first. A plan may span layers only when it defines explicit boundaries, orders warehouse work before API work and API work before frontend work, and validates each boundary independently.

Treat the warehouse, API, analytics UI, and social functionality as connected product layers with explicit contracts. Make current-phase decisions that leave a clear path for later phases, but do not prematurely implement a later phase unless an approved plan places it in scope. Prefer durable data identities, provenance, metadata, and queryable semantics that future API and web clients can safely use.

Throughout every phase, preserve source fidelity, reproducibility, safe replay, explicit data-layer boundaries, privacy and security, and testable evidence. The application must distinguish provider-published facts from derived analysis and retain enough provenance for users to understand and responsibly share results.

Read `README.md`, `pyproject.toml`, and the relevant source, tests, and documentation before making architectural changes. Preserve unrelated user changes in the working tree.

## Reference documentation is part of the contract

Remain actively aware of `docs/reference/`; it is operational and engineering guidance, not background reading. Before planning or changing related behavior, read the applicable reference document in full and reconcile the implementation, tests, and plan with it:

- `docs/reference/TESTING_CONTRACT.md` defines test layers, markers, fixtures, isolation, infrastructure expectations, quality gates, and the behavioral catalog. Read it before changing production behavior, test architecture, markers, fixtures, or CI expectations.
- `docs/reference/ADDING_A_DATA_SOURCE.md` defines the required adapter contract and checklist. Read it before planning, implementing, or reviewing any new public-data source.
- `docs/reference/BETA_RESET_REINGESTION.md` defines safe warehouse bootstrap, reset, dependency-order ingestion, and completion checks. Read it before changing bootstrap, migrations, manifests, ingestion ordering, replay, or operational re-ingestion behavior.
- `docs/reference/CI_EVIDENCE_MAP.md` defines which automated checks prove each repository contract. Read it when adding behavior, selecting validation, changing CI, or claiming a plan complete.

Treat these documents as living contracts. When implementation intentionally changes a documented contract, update the relevant reference and its tests in the same change. Do not allow plans, code, tests, CI, and reference documentation to silently diverge. If a plan conflicts with a reference contract, investigate the repository evidence and resolve the conflict explicitly before implementation.

## Test-driven design

Test-driven design is a primary engineering requirement, not a final verification activity. For each behavior or contract change:

1. Identify the applicable behavior and quality gate in `docs/reference/TESTING_CONTRACT.md` and `docs/reference/CI_EVIDENCE_MAP.md`.
2. Write or update the smallest deterministic test that expresses the required behavior and important failure path. When introducing new behavior, confirm the test fails for the expected reason before implementing it.
3. Implement the smallest coherent change that satisfies the test.
4. Run the focused test immediately and do not continue while it is failing.
5. Run the affected component or contract suite after the focused test passes. Fix regressions before beginning the next implementation step.
6. Refactor only with passing tests, then rerun the focused and affected suites.
7. At each plan milestone and before moving a plan to `needs_review/`, run the broadest practical applicable suites and record commands, results, and any environment-limited checks in the plan.

Maintain a green test state throughout implementation. Never accumulate known failures for cleanup at the end, weaken assertions to make a test pass, or replace meaningful behavioral checks with implementation-detail assertions. A skipped, deselected, quarantined, or unexecuted test is not successful evidence. If a required test cannot run, stop work that depends on its result or document the exact environmental blocker; never assume it passes.

## Implementation plan workflow

The implementation backlog lives under `docs/plans/`:

- `to_do/`: approved plans that have not been claimed.
- `in_progress/`: the plan currently being implemented or a plan that was interrupted.
- `needs_review/`: implementation the agent judges complete and ready for human review.
- `completed/`: plans accepted by a human reviewer. Agents do not move plans here unless the user explicitly asks.

The folder containing a plan is its authoritative workflow state. Keep any status, checkpoint, remaining-work checklist, and last-updated date inside the plan consistent with that folder.

When the user asks to implement the plans, work the backlog as a loop rather than stopping after one plan:

1. Inventory all Markdown plan files in `docs/plans/in_progress/` and `docs/plans/to_do/`. Do not treat `docs/plans/README.md` as a plan.
2. Resume plans in `in_progress/` before claiming new work. If more than one exists, prefer a dependency-unblocking plan, then the oldest plan by Git history, then filename order.
3. If `in_progress/` is empty, select an unblocked plan from `to_do/` using the dependencies and delivery order documented in the plans and `docs/plans/README.md`. Move exactly one selected plan to `in_progress/` before implementation and update its internal status/checkpoint.
4. Read the entire selected plan. Inspect the repository to confirm its assumptions, dependencies, acceptance criteria, and current implementation state. Existing code is evidence to verify, not proof that a criterion is complete.
5. Implement the plan end to end using the test-driven design loop above. Keep the plan current as work proceeds: mark only tested and verified items complete, record material decisions, and identify the next pickup when work remains.
6. Validate at every meaningful implementation step. Do not proceed past a failing focused or affected suite. Before completion, validate using the definition of done below, fix failures that are within the plan's scope, and rerun the relevant focused and broader checks.
7. When the definition of done is satisfied, add or refresh a concise implementation-evidence section in the plan. Include the delivered behavior, important files or migrations, tests and commands run with their results, and any non-blocking review notes. Set its status to ready for review, clear the next-pickup field, ensure no in-scope checklist item remains open, and move the plan to `needs_review/`.
8. Repair links affected by a plan move and keep `docs/plans/README.md` accurate when its links or delivery status changed.
9. Re-inventory both active folders and repeat. Do not rely on an inventory captured before the latest implementation because completing a plan can unblock or change another plan.

The backlog loop is complete only when both `docs/plans/to_do/` and `docs/plans/in_progress/` contain no Markdown plan files and every plan handled by the loop is in `docs/plans/needs_review/`. Report the final inventory and validation results. Do not claim the loop is complete merely because code exists, a checklist is checked, or a plan was already located in `needs_review/`.

## Plan definition of done

A plan may move to `needs_review/` only when all of the following are true:

- Every stated acceptance criterion and in-scope deliverable is implemented and backed by inspectable repository evidence.
- Relevant automated tests were designed alongside or before the implementation, added or updated, and pass. Tests cover meaningful failure paths, replay/idempotency behavior, and contract boundaries where the plan calls for them.
- Relevant formatting, lint, build, schema, migration, and documentation checks pass.
- Applicable requirements in `docs/reference/TESTING_CONTRACT.md`, `docs/reference/ADDING_A_DATA_SOURCE.md`, `docs/reference/BETA_RESET_REINGESTION.md`, and `docs/reference/CI_EVIDENCE_MAP.md` are satisfied and remain synchronized with the implementation.
- Documentation, configuration examples, fixtures, migrations, manifests, and operational instructions affected by the change are current.
- No unresolved in-scope TODO, placeholder, skipped requirement, or known defect remains.
- Secrets, credentials, and sensitive source parameters are absent from code, fixtures, logs, captured request metadata, and committed files.
- The implementation does not silently weaken the plan. Any necessary scope or acceptance-criteria change is explicitly approved by the user and recorded in the plan.
- Validation evidence is recorded in the plan, including limitations of checks that could not run locally.

Use judgment to choose evidence, not to waive requirements. If an acceptance criterion is ambiguous, resolve it from repository contracts and documented intent where possible. Ask the user only when the choice would materially change product behavior or scope.

If a plan is blocked by a missing decision, credential, unavailable external system, or unmet dependency, do not move it to `needs_review/` and do not mark the backlog complete. Keep it in `in_progress/`, document the blocker and completed evidence, continue with other genuinely independent plans if useful, and report the exact action needed to unblock it.

## Validation commands

Run the smallest relevant checks while iterating, then the broadest practical suite before moving a plan to `needs_review/`.

- Default Python suite: `pytest`
- Lint: `ruff check .`
- Focused suites: use the targets in `Makefile`, including `test-unit`, `test-etl`, `test-api`, `test-dags`, and the relevant integration or end-to-end target.
- Web unit tests: `npm --prefix apps/web run test:unit`
- Web lint and build: `npm --prefix apps/web run lint` and `npm --prefix apps/web run build`

Integration, database, Redis, Martin, external-source, performance, browser, and composed-service checks may require optional dependencies or disposable services. Run them when the changed contract depends on them. Never treat an unavailable environment as a passing check; record what was and was not executed.

## Engineering expectations

- Preserve raw source evidence before parsing or normalization.
- Keep raw capture, control state, silver normalization, gold publication, and serving/API responsibilities explicit.
- Prefer deterministic offline fixtures for source behavior. Live external calls supplement contract tests; they do not replace replayable tests.
- Preserve idempotency, revision history, geography identity, units, suppression/missing semantics, and lineage.
- Do not infer geography from names when authoritative codes or mappings are required.
- Do not silently convert suppressed, missing, invalid, or non-numeric values to zero.
- Keep provider adapters isolated and shared contracts provider-neutral.
- Avoid unrelated refactors while implementing a plan.
- Do not delete or overwrite user work. Do not commit, push, deploy, rotate credentials, or modify external systems unless the user explicitly asks.

## Handoff

At the end of each response, state which plan is in progress or was moved to `needs_review/`, summarize implementation and validation evidence, list remaining active plans, and call out blockers or checks not run. Continue the backlog loop in the same run when the user asked for the whole backlog; a plan-sized milestone is a progress update, not a stopping condition.
