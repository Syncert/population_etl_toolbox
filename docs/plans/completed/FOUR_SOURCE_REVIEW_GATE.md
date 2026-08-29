# Four-source warehouse review gate (retired 2026-08-28)

> **Retired and archived.** This gate was approved and removed from the
> dispatch graph on 2026-08-28. It carries no dispatcher frontmatter, so
> `load_plans` skips it as readable guidance rather than parsing it as a gate
> that still needs a decision; a gate is never satisfied by its folder, so
> leaving its `id: four-source-review` block in place here would block
> `warehouse-data-quality`, `data-product-e2e`, and `api-platform` forever.
> Its id has been removed from those three plans' `depends_on`. The recorded
> decision is at the end of this document.

This gate stopped the dispatcher once the CDC, FBI Crime, USDA NASS Crop, and
Census PEP pipelines had all been implemented, verified, and integrated.
Nothing that consumed those four sources could start until a human approved it.

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

## Recorded decision

**Approved 2026-08-28 by Syncert (repository owner), on review performed by
Claude Code at the owner's direction.**

No dispatcher run was in flight, so there was no `.claude/plan-runner-state.json`
to write into. This section is the record instead, and the gate has been retired
from the graph as described at the top of this document.

### Note attached to the approval

Machine-verifiable precondition: **met.** `dag-parse` run 102 on `main` at
`1f33b38`, Airflow 2.9.3 on Python 3.11 against pinned PostGIS 16.14 —
113 passed, 0 skipped, 0 errors in 134.35 s. All ten production DAGs executed
as real `DagRun`s in warehouse order, `cdc_ingest`, `fbi_ucr_ingest`,
`usda_nass_crop_ingest`, and `census_pep_ingest` among them. All thirteen
workflows were green on that commit.

### Checklist as reviewed

- [x] Orchestrated DAG suite passes on the integration branch — as above.
- [x] Provider grain, identity, units, suppression, and revision semantics
      preserved per source — each source owns symmetric capture, client,
      config, metadata, registry, and replay suites plus a disposable-PostGIS
      pipeline test, and each plan's own acceptance evidence was re-read
      against its implementation.
- [x] Geography resolves through the shared versioned dimensions — all four
      route through `silver_ref.resolve_provider_geography` into
      `silver_ref.geography_resolution` and `dim_geo_entity`. No source-local
      geography table exists in migrations 009 through 012. FBI additionally
      uses `bridge_geo_relationship_version` for its agency-to-county
      relationship, consistent with its plan's decision that county is a filter
      relationship rather than an observation grain.
- [x] Capture, replay, and quarantine behave consistently — all four import the
      same `CaptureControl`, `CaptureReceipt`, `ResponseCapture`, and
      `persist_response_capture` from the shared `capture.py`, and each owns its
      own quarantine table rather than a shared one.
- [x] One publication contract — each publishes `measure_export`,
      `metric_publisher`, and `latest_release_observation`, and
      `glossary/harvest.py` discovers publishers purely by the 17-column view
      shape, so nothing source-specific is wired into the glossary path.
      `glossary_harvest` and `glossary_reconciliation` run inside the same
      orchestrated DagRun.
- [x] No source-specific compensation in shared code — shared modules carry no
      per-source branching. One deliberate exception is recorded rather than
      waived: `silver_ref/geography_contract.py` holds a per-provider allowlist
      of supported geography types. It is a declared capability table, not
      behavioral branching, but it does mean onboarding a source requires
      editing shared reference code.
- [~] Live provider contracts — every source now owns a module in the scheduled
      external tier, and `REQUIRED_SCHEDULED_CREDENTIALS` fails a scheduled run
      missing any source key rather than skipping it. **Two have never executed
      against their provider:** FBI UCR and USDA NASS, which need
      `FBI_CDE_API_KEY` and `USDA_NASS_API_KEY` repository secrets. Census ACS,
      PEP, BLS, FRED, and CDC have run live. This is the one item accepted
      incomplete, deliberately: the tier is scheduled and credentialed, never a
      pull-request gate, so it was not a precondition of this review.
- [x] Plans' acceptance criteria met rather than reinterpreted — the four plans'
      evidence records were corrected in the same change set, replacing the
      stale blocked-DagRun entries with the run that actually happened. Census
      PEP's two remaining checkboxes are external-deployment operator steps,
      unchanged in wording and moved under an explicit post-acceptance heading.
- [x] Integration branch coherent — migrations 010, 011, and 012 create no
      duplicate objects, issue no `ALTER` or `DROP` against shared objects, and
      register one distinct `source_code` each. All three sit in
      `warehouse_manifest.json` in the `source` phase between migration 009 and
      the gold phase.

### Follow-up carried out of this gate

1. Add `FBI_CDE_API_KEY` and `USDA_NASS_API_KEY` repository secrets, then run
   the `external-contract` workflow. Until then its scheduled run fails at the
   credential check by design, rather than skipping two sources quietly.
2. `silver_ref/geography_contract.py`'s per-provider allowlist is shared code
   that every new source must edit. Worth revisiting when the next source is
   onboarded.
