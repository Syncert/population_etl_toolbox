---
id: four-source-review
kind: gate
depends_on:
  - cdc-illness
  - fbi-crime
  - usda-crop
  - census-pep
---

# Four-source warehouse review gate

This gate stops the dispatcher once the CDC, FBI Crime, USDA NASS Crop, and
Census PEP pipelines have all been implemented, verified, and integrated.
Nothing that consumes those four sources may start until a human approves it.

## Why this gate exists

The four source plans are implemented in parallel by independent workers. Each
one is verified against its own acceptance criteria, but nothing in the
automated run checks the questions that only matter once all four exist
together:

- whether the four sources agree on shared geography, time, and revision
  semantics;
- whether their gold products can be compared without the API compensating for
  a source-specific decision;
- whether four separately-reasoned adapters have quietly diverged from
  `docs/reference/ADDING_A_DATA_SOURCE.md`; and
- whether the combined warehouse is worth building the API platform on.

A per-plan test suite cannot answer those. A human looking at all four
diffs together can, and this is the cheapest point to answer them — before the
warehouse-quality, end-to-end, and API plans build on the result.

Census PEP belongs here rather than in a gate of its own. It resolves the same
shared geography dimensions, publishes through the same glossary publisher
contract, and runs in the same orchestrated DAG order as the other three, so
the cross-source questions above are exactly as unanswerable for PEP alone as
they are for the others.

## Required evidence before review

A checklist alone asks a person to certify something no one has measured. This
gate therefore has a machine-verifiable precondition: the orchestrated DAG
suite must pass on the integration branch before the gate is reviewed.

That suite is `tests/dags/test_dag_pipeline_execution.py`. Two entry points
select it, and either one satisfies this gate:

```powershell
# Local runner: selects the module directly against a disposable warehouse.
./tests/run.ps1 dag-pipeline
```

```bash
# CI owner: the dag-parse job, which selects the same module as part of the
# DAG tier. Its command-line -m overrides the default marker filter in
# pyproject.toml, and the job supplies both RUN_DAG_TESTS and the PostGIS
# service, so the orchestrated tests run rather than skip.
RUN_DAG_TESTS=1 pytest -m dag tests/dags/
```

The local runner is the narrower selection and the CI job is the superset; the
CI job is the one that runs on every push to the integration branch, on pinned
Airflow 2.9.3 / Python 3.11 against pinned PostGIS 16, so it is the authority
when the two disagree about environment. A local run on a contaminated Airflow
install is not evidence either way.

That suite runs every DAG in `dags/` as a real Airflow `DagRun` against a
disposable PostGIS warehouse, driving a bounded provider sample from capture
through replay to publication. It is the closest automated equivalent of the
first production Airflow run, and it fails if any task in any pipeline fails.
`test_every_production_dag_is_covered_by_this_suite` additionally fails if a
production DAG exists that the orchestrated run does not execute, so a new
source cannot quietly escape this gate's evidence.

Attach the result to the approval note. If the suite has not been run, or does
not pass, the answer is to fix the pipeline rather than to approve the gate on
the strength of the per-plan suites — those already passed for each source in
isolation, which is exactly what this gate exists to look past.

### Live provider contracts

Every source in this gate must also own a live contract module under
`tests/external/`, registered in the `external-contract` workflow with its
credential. That tier is scheduled and credentialed, never a pull-request
gate, so it is not a precondition of this review in the way the orchestrated
run is. What this gate does check is that no source is missing from it: a
source with no external module drops out of live coverage silently, and
`tests/support/external.py::REQUIRED_SCHEDULED_CREDENTIALS` now fails a
scheduled run that is missing any source's key rather than skipping it.

Record the date of the last green `external-contract` run per source in the
approval note, and record explicitly any source whose live contract has not yet
executed against the provider.

## What a reviewer must confirm

- [ ] The orchestrated DAG suite passes on the integration branch, with every
      DAG reaching a successful DagRun, under either entry point above.
- [ ] Each source preserves provider grain, identity, units, suppression, and
      revision semantics without inventing a value the provider did not publish.
- [ ] The four sources resolve geography through the shared versioned
      dimensions rather than source-local mappings.
- [ ] Capture, replay, and quarantine behave consistently across all four.
- [ ] Gold products and glossary entries follow one publication contract.
- [ ] No source-specific compensation has leaked into shared code.
- [ ] Every source has a live contract module in the scheduled external tier,
      and any source whose live contract has not yet run is named in the note.
- [ ] The plans' own acceptance criteria are met rather than reinterpreted.
- [ ] The integration branch is coherent: no conflicting migrations, duplicated
      shared objects, or contradictory DDL ordering across the four merges.

## Approving or rejecting

From the repository root, once the run reports the gate is awaiting review:

```powershell
./tools/Invoke-ClaudePlans.ps1 -Action approve -Gate four-source-review `
    -By "your name" -Note "orchestrated DAG suite green; reviewed all four source diffs"

./tools/Invoke-ClaudePlans.ps1 -Action reject -Gate four-source-review `
    -By "your name" -Note "CDC and PEP disagree on county vintage handling"
```

Approving lets the dependent plans dispatch on the next tick. Rejecting blocks
every dependent plan and ends the run, so the problem is fixed deliberately
rather than built upon. `-Action reopen` clears a recorded decision if it was
made in error.

The decision, who made it, when, and the note are recorded in the run-state
file, so a later reader can see the checkpoint was actually cleared by a
person. When no dispatcher run is in flight there is no run-state file to write
into; record the decision by retiring this gate to `docs/plans/completed/` with
its decision section filled in, and remove its id from every dependent plan's
`depends_on`. Deleting the file alone breaks the graph: `validate_graph`
rejects a dependency naming a plan that no longer exists.
