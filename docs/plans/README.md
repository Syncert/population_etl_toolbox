# Implementation plan execution workflow

This directory is the execution queue for repository work. It describes how an
agent discovers, claims, implements, validates, and hands off plans. The plans
themselves define what must be delivered, their dependencies, and their
acceptance criteria; this README does not duplicate that backlog.

Agents should treat the repository as persistent execution state. Folder
location, the active plan's checkpoint, the working tree, and test evidence
must be sufficient for another agent to continue after an interruption or
context reset.

## Workflow states

The folder containing a plan is its authoritative state:

```text
to_do/ -> in_progress/ -> needs_review/ -> completed/
```

- `to_do/`: approved, unclaimed plans.
- `in_progress/`: claimed work or work paused at a documented blocker.
- `needs_review/`: implementation-complete plans awaiting human review.
- `completed/`: plans accepted by a human reviewer.

Agents may move plans through `to_do/`, `in_progress/`, and `needs_review/` as
the conditions below are met. They must not move a plan to `completed/` unless
the user explicitly asks them to record human acceptance.

Do not infer plan state from a checklist, prose status, or existing code when
it conflicts with the containing folder. Reconcile the plan's internal status
and checkpoint with its folder before continuing.

## The agentic backlog loop

When asked to execute the plans, repeat this loop until its stop conditions are
met. Completing one plan is a checkpoint, not the end of a backlog run.

1. **Re-inventory the queue.** List every Markdown plan in `in_progress/` and
   `to_do/`. Do this at the beginning of the run and again after every plan
   transition; a completed plan may unblock or reprioritize another plan.
2. **Resume before claiming.** Resume work already in `in_progress/`. If more
   than one plan is present, prefer dependency-unblocking work, then the oldest
   plan by Git history, then filename order.
3. **Select an unblocked plan.** If no plan is in progress, read candidate plans
   only far enough to resolve their declared dependencies and delivery order.
   Select one whose prerequisites are satisfied. Respect the repository's
   architecture order: stable warehouse objects, then API contracts, then web
   features.
4. **Claim exactly one plan.** Move the selected file from `to_do/` to
   `in_progress/` before implementation. Update its internal status,
   last-updated value, and next-pickup checkpoint in the same change.
5. **Read the contract.** Read the entire active plan and the applicable
   documents under `docs/reference/`. Inspect only the implementation, tests,
   migrations, configuration, and documentation needed to verify the plan's
   assumptions and acceptance criteria.
6. **Establish the gap.** Run the smallest deterministic test or executable
   check that demonstrates the missing behavior. Existing code is evidence to
   inspect, not proof that the plan is complete.
7. **Implement in tested increments.** For each behavior, add or update the
   smallest meaningful test, confirm the expected failure when introducing new
   behavior, implement the smallest coherent change, and immediately rerun the
   focused test. Do not carry known failures into later work.
8. **Validate the affected boundary.** After focused tests pass, run the
   applicable component, contract, lint, build, schema, migration, replay, and
   documentation checks. Expand validation in proportion to the changed
   contract and fix in-scope regressions before continuing.
9. **Persist progress.** At every meaningful checkpoint, update the active plan
   with verified items, decisions, commands and results, remaining work, and a
   precise next pickup. Inspect the working-tree diff after material edits.
10. **Hand off for review.** When every completion condition below is satisfied,
    record concise implementation evidence, set the plan status to ready for
    review, clear its next-pickup field, move it to `needs_review/`, and repair
    links affected by the move.
11. **Loop.** Re-inventory `in_progress/` and `to_do/`, then resume at step 2.

## Plan checkpoint contract

An active plan should make its current execution state obvious without relying
on conversation history. Maintain these fields or equivalent sections inside
the plan:

- workflow status consistent with the containing folder;
- last-updated date;
- declared dependencies and whether they are satisfied;
- acceptance criteria and implementation checklist;
- verified decisions and completed evidence;
- exact validation commands and outcomes;
- remaining work, next pickup, or a concrete blocker.

Mark an item complete only after repository evidence and the required checks
support it. Do not weaken, reinterpret, or silently remove an acceptance
criterion. A material scope change requires user approval and must be recorded
in the plan.

## Completion gate

A plan is ready for `needs_review/` only when all of the following are true:

- every in-scope deliverable and acceptance criterion has inspectable
  implementation evidence;
- meaningful success, failure, boundary, and replay or idempotency behavior is
  tested where applicable;
- focused tests and the broadest practical affected suites pass with no
  unexpected skips or xfails;
- applicable formatting, lint, build, package, schema, migration, and
  documentation checks pass;
- implementation, tests, CI ownership, reference contracts, fixtures,
  manifests, examples, and operating instructions remain synchronized;
- no unresolved in-scope TODO, placeholder, known defect, credential, secret,
  or sensitive captured value remains; and
- the plan contains a concise evidence record, has no remaining checklist item,
  and has no next pickup.

Use the test layers and quality gates in
[`TESTING_CONTRACT.md`](../reference/TESTING_CONTRACT.md), and map changed
contracts to their authoritative checks with
[`CI_EVIDENCE_MAP.md`](../reference/CI_EVIDENCE_MAP.md). Source-adapter work
must also follow
[`ADDING_A_DATA_SOURCE.md`](../reference/ADDING_A_DATA_SOURCE.md); bootstrap,
migration, ingestion-order, and replay work must follow
[`BETA_RESET_REINGESTION.md`](../reference/BETA_RESET_REINGESTION.md).

An unavailable environment is not passing evidence. Record the exact command,
why it could not run, and which conclusion remains unverified. If that evidence
is required by an acceptance criterion, keep the plan in `in_progress/`.

## Blocked work

A plan is blocked only when safe in-scope investigation cannot resolve a
required decision, credential, external dependency, environment, or unmet plan
dependency.

When blocked:

1. keep the plan in `in_progress/`;
2. record the blocking condition, evidence gathered, completed work, and the
   exact action needed to resume;
3. preserve a focused next-pickup instruction;
4. continue with another genuinely independent plan only when doing so respects
   dependencies and does not create conflicting active changes; and
5. report the blocker to the user without representing the plan or backlog as
   complete.

Difficulty, repository size, incomplete exploration, or an optional broader
check are not blockers. Prefer a targeted experiment or the smallest justified
implementation step over open-ended inspection.

## Recovery after interruption

Resume from repository evidence rather than reconstructing prior conversation:

1. inventory `in_progress/` and `to_do/`;
2. read the active plan's checkpoint and evidence sections;
3. inspect `git status` and the relevant diff without discarding unrelated user
   changes;
4. rerun the last focused validation when its result is uncertain; and
5. continue from the recorded next pickup.

Do not redo verified work unless the repository or a failing check provides new
contradictory evidence.

## Termination conditions

For a full-backlog request, the loop succeeds only when `to_do/` and
`in_progress/` contain no Markdown plan files and every plan handled by the run
is in `needs_review/`. Report the final folder inventory and validation results.

Stop earlier only when a genuine blocker requires user action, continuing would
risk unrelated work or violate a repository contract, or the environment can no
longer read, modify, or validate the repository reliably. Before stopping,
persist enough state in the active plan for the next agent to resume directly.
