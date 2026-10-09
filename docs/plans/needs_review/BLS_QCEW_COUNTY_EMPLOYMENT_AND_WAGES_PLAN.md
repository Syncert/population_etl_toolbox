---
id: bls-qcew-county-wages
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/bls_qcew -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_bls_qcew_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# BLS QCEW: county employment and wages by industry

## Status

Ready for review. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Deliverables 1 to 6 (adapter, registry, silver, gold, serving, quality and
operations) are on branch `feat/bls-qcew`, cut from `main`. Deliverable 7,
the county-page cards, is on `feat/bls-qcew-cards`, which is built on
`feat/place-pages` (WEB-125) with `feat/bls-qcew` merged. Merge those two
first.

## Why

"What do people do here, and what does it pay" is the most asked question
the almanac cannot answer. LAUS says how many residents are unemployed; it
says nothing about jobs located in the county or what they pay. The
Quarterly Census of Employment and Wages publishes establishments,
employment, and total and average weekly wages by industry for every county,
quarterly, from unemployment-insurance records rather than a survey. The
existing BLS configuration already notes that QCEW must not be forced into
the series-identifier model used for LAUS and CES
(`bls/config.py`), so it is onboarded as its own adapter package.

## What exists

- The BLS adapter (LAUS county, CES, CPI, JOLTS national) with its
  program-aware series model; its documentation explicitly separates
  household measures from establishment measures.
- The source-adapter starter (`docs/templates/source-adapter/`) and the
  checklist in `docs/reference/ADDING_A_DATA_SOURCE.md`.
- Shared raw capture and control plane, geography resolution by county
  FIPS, and the glossary publisher contract.

## Deliverables

1. **Adapter package** `src/data_ingestion_toolbox/bls_qcew/` from the
   starter: config without import-time I/O; a client for the QCEW open-data
   CSV slices (one file per area and period, no credential expected; verify
   against the official QCEW open-data documentation during implementation);
   capture-first raw storage with checksum and request fingerprint; a
   versioned parser contract for the published CSV layout.
2. **Scope registry.** Explicit registration of ownership (private, and
   total covered), aggregation level (county, state, national), industry
   level (total, NAICS sector), and the period range; nothing is requested
   that is not registered.
3. **Silver.** Facts keyed to the shared time and geography dimensions with
   industry, ownership, and aggregation level as dimensions; disclosure
   suppression codes preserved as withheld values, never zero; the
   provider's annual-average records kept distinct from quarterly records.
4. **Gold.** Deterministic publication of establishments, employment by
   month, total quarterly wages, and average weekly wage, per (geography,
   industry, ownership, period), with units and the establishment-based
   observation basis stated in the publisher contract so it can never be
   confused with LAUS's household basis.
5. **Serving.** Discovery and observation dispatch entries in
   `apps/api/registry.py`; the consumer guide gains a section on reading a
   QCEW row (ownership, industry, basis, suppression); the OpenAPI snapshot
   is updated.
6. **Quality and operations.** Declared data-quality rules (suppression
   preserved, period continuity, county identity resolution), a DAG, an
   operations guide under `docs/user-guides/`, a live source-contract module
   under `tests/external/`, and bootstrap and reset instructions.
7. **Web, last.** The Work and Money chapter of a county page gains "Jobs
   located here" cards and an industry-mix table labeled with the
   establishment basis, beside the LAUS cards labeled with the household
   basis; the two are never summed or shown as one series.

## Acceptance criteria

- Configuration imports without network, database, or secret access; a
  checked-in county CSV fixture is captured byte-for-byte and replays
  offline into silver with suppressed cells preserved as withheld.
- A malformed fixture is captured and quarantined with a sanitized record.
- Gold publishes the four measures per (geography, industry, ownership,
  period) with units and the establishment basis in the publisher contract;
  the glossary contract test passes.
- Re-running a slice is idempotent; a changed provider file for the same
  slice retains both checksums.
- `/api/v1/observations` serves a QCEW metric for a county fixture with
  industry and ownership dimensions on the row; capabilities advertise the
  source; the consumer guide documents the row semantics.
- Quality rules run against the fixtures; the DAG parses; the external
  contract module is registered in the scheduled credentials map and the
  external-contract workflow.
- The county page shows QCEW beside LAUS with distinct basis labels and no
  combined figure; a browser scenario asserts both labels.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded
  here.

## Open items to resolve during implementation

- The exact open-data slice URL pattern, the annual-average period code, and
  the published field list and layout version (official BLS QCEW
  documentation first).
- Whether to onboard NAICS sector level in the first release or total
  covered employment only; sector level is what the industry-mix table
  needs, so prefer it if slice volume allows.

## Decisions

- **Slices, verified 2026-10-06.** One CSV per (year, quarter `1`-`4` or
  annual `a`, industry) at
  `https://data.bls.gov/cew/data/api/<year>/<period>/industry/<code>.csv`,
  with `31-33` spelled `31_33`. The interface starts at 2014 (2013 answers
  404), answers 404 for a quarter not yet published, takes no credential,
  and refuses requests without a descriptive `User-Agent`. The annual
  layout has its own columns (`annual_avg_estabs`, `annual_avg_emplvl`,
  `total_annual_wages`, `annual_avg_wkly_wage`).
- **Scope.** NAICS sectors are onboarded in the first release (open item):
  the total of all industries for ownership 0 (total covered) and 5
  (private), and the 21 sectors for ownership 5, at the national, state and
  county aggregation levels (10-14, 50-54, 70-74). A slice's other rows are
  counted, not loaded: MSAs and CSAs, other ownerships, size classes, and
  the `SS999` "unknown county" areas.
- **Values.** Disclosure `N` is `withheld` and `-` is `not_published`. Both
  carry no number, because the file writes `0` there. Monthly employment is
  three month-dated rows. Annual averages are separate measures, never
  derived from the quarters.
- **Identity.** A metric is `BLS_QCEW:<measure>:<industry>:<ownership>`. The
  display name ends "(jobs located here)", and every row carries
  `observation_basis`, so a QCEW figure cannot be read as LAUS's residents.
- **Cadence.** Monthly. An ordinary run asks for the last six calendar
  quarters and their years' annual averages, and unpublished ones are
  recorded empty. `{"history": true}` sweeps from 2014. The one-slot
  `bls_qcew_api` pool serializes the requests.

## Evidence (2026-10-06, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2188 passed, including
  `tests/unit/bls_qcew` (9).
- Database: `tests/integration/database/test_bls_qcew_capture_replay.py` --
  6 passed. Covered: gold with withheld cells, publisher and harvest, rerun
  and changed file, quarantine, 404 period and schema reapply, and
  `DQ-QCEW-002` passing the fixtures and failing on a lost row.
  `tests/integration/api/test_catalog_serving_agreement.py` -- 21 passed with
  a QCEW fixture (DB-043/DB-044).
- End to end: `tests/e2e/test_bls_qcew_pipeline.py` serves Kent County,
  Delaware employment by month with industry, ownership and basis on the
  row, and withheld cells as nulls.
- Live: `tests/external/test_bls_qcew_source_contracts.py` -- 5 passed
  against data.bls.gov.
- DAG: `pytest -m dag tests/dags` in the scheduler container -- 156 passed.
  `test_dag_pipeline_execution.py` passed 4 of 4 on its own, with
  `bls_qcew_ingest` in the orchestrated run. In the combined tier it hits
  the same `fred_ingest.mark_slices_planned` failure it hits on other
  branches, before this DAG runs.
- Integration: `tests/integration/api` -- 186 passed on a fresh database.
  `tests/integration/database tests/e2e` -- 268 passed and 2 failed. The
  foundation table list now names `control.bls_qcew_slice` and passes. The
  PEP teardown node counts every `CENSUS_PEP` capture in the database, so it
  fails after an earlier database node leaves PEP captures behind; it passes
  on its own, and it fails the same way on other branches.
- `ruff check .` and `ruff format --check .` clean; schema snapshot
  regenerated; OpenAPI snapshot unchanged (no new route).

### County-page cards (WEB-135, `feat/bls-qcew-cards`)

- Work and Money gains "Jobs located here" (`BLS_QCEW:employment:10:0`) and
  "Average weekly wage of jobs located here" beside the LAUS unemployment
  rate. The QCEW cards read "Jobs located here, counted by employers
  (QCEW)" and the LAUS card reads "Residents who work, counted where they
  live (LAUS)". No slot mixes the two.
- An industry-mix table lists private jobs located here for the 21 NAICS
  sectors, each the published figure for the newest month. Its caption
  states the establishment basis. A withheld sector says so, and nothing is
  summed or divided.
- Evidence: `npm --prefix apps/web run test:unit` -- 740 passed; `lint`
  clean; `test:browser` -- 208 passed, including the new places scenario;
  `check:bundle` keeps every route within its budget.

## Checkpoint

Implementation complete; awaiting human review of `feat/bls-qcew`, then
`feat/bls-qcew-cards`.
