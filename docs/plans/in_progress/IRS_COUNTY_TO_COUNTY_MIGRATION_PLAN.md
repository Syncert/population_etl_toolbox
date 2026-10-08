---
id: irs-county-migration
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/irs_migration -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_irs_migration_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# IRS county-to-county migration flows

## Status

In progress. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Deliverables 1 to 5 are on branch `feat/irs-county-migration`, cut from
`main`. Deliverable 6, the People and Change chapter lists, needs the place
pages from `feat/place-pages` (WEB-125).

## Why

PEP tells a county its net migration. The IRS Statistics of Income migration
data tells it where people came from and where they went: county-to-county
inflows and outflows by number of returns, number of exemptions, and
adjusted gross income, annually, derived from address changes on tax
returns. It turns one number into a story with named origins, which is both
the most engaging section of the People chapter and a natural video. It is
also the first flow-shaped dataset in the warehouse, so the dimensional
model must gain an origin-destination fact shape without bending the
existing one-geography fact pattern.

## What exists

- PEP components of change, including domestic and international migration,
  at county grain.
- The source-adapter starter and checklist; geography resolution by county
  FIPS; the shared time dimension.

## Deliverables

1. **Adapter package** `src/data_ingestion_toolbox/irs_migration/` from the
   starter: a client for the published county inflow and outflow files per
   filing-year pair (exact file names, layouts, and the meaning of the
   aggregate rows such as same-state, other-state, foreign, and
   non-migrant verified against the official SOI documentation); no
   credential expected.
2. **Flow fact shape.** A silver fact keyed to the time dimension and two
   geography keys (origin, destination) with returns, exemptions, and
   adjusted gross income as measures; the SOI suppression rule (small flows
   suppressed and rolled into aggregate rows) preserved as withheld and the
   aggregate rows kept as their own labeled subjects, never redistributed.
   The dimensional contract for a two-geography fact is recorded as a
   decision beside ADR-0001.
3. **Gold.** Deterministic publication of flows per (origin, destination,
   filing-year pair, measure) and the per-county totals the files carry
   (total inflow, total outflow); no net figure is computed here unless the
   files publish one.
4. **Serving.** A flow resource (route shape decided against the consumer
   guide, additive) answering "top origins into this county" and "top
   destinations from this county" for one year with the suppression and
   aggregate rows visible; discovery entry; the consumer guide gains a
   section on reading a flow row (returns are filers, exemptions
   approximate people, income is AGI of movers); OpenAPI snapshot updated.
5. **Quality and operations.** Quality rules (suppression preserved,
   origin and destination both resolve, totals reconcile to the files' own
   totals within the documented tolerance), DAG, operations guide, external
   contract module, bootstrap and reset instructions.
6. **Web, last.** The People chapter gains "Where people came from" and
   "Where people went" lists for the latest year with the withheld note,
   and the Change chapter links them beside PEP net migration with the
   statement that the two sources measure different populations.

## Acceptance criteria

- Configuration imports without I/O; checked-in inflow and outflow fixtures
  for one state replay offline into the flow fact with suppressed flows
  withheld and aggregate rows labeled; a malformed fixture is quarantined.
- The two-geography fact shape is recorded as a decision document and
  covered by a unit test that refuses a flow whose origin or destination
  does not resolve.
- Gold publishes flows and file totals with no computed net figure; the
  glossary contract test passes.
- Idempotent re-run; both checksums retained on a changed file.
- The flow resource answers top origins and destinations for a county
  fixture with withheld counts visible; capabilities advertise the source;
  consumer guide and OpenAPI snapshot updated.
- Quality rules, DAG parse, and external contract registration are in place.
- The county page shows both lists with the withheld note and the
  different-populations statement beside PEP; a browser scenario asserts
  the statement.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded
  here.

## Open items to resolve during implementation

- Year-pair labeling (filing years versus tax years) and how it maps to the
  shared time dimension; label exactly as SOI does.
- Whether the flow resource belongs under observations or a new namespace;
  it is provider-published data, not derived, so it must not be labeled
  derived.

## Decisions

- **Files, verified 2026-10-06** against SOI's 2022-2023 documentation and
  files: `https://www.irs.gov/pub/irs-soi/countyinflow<YY><YY>.csv` and
  `countyoutflow<YY><YY>.csv`, one pair per pair of filing years, with the
  columns `y2_statefips, y2_countyfips, y1_statefips, y1_countyfips,
  y1_state, y1_countyname, n1, n2, agi` (inflow; `y1`/`y2` exchanged for
  outflow). The 2021-2022 file writes codes without padding (`10,1`), so
  codes are read as numbers and padded. No credential.
- **Years registered:** 2018-2019 to 2022-2023. From 2018-2019 SOI deletes
  county counts below 20 rather than moving them into another county's
  category and dropped the state totals, so earlier files follow different
  rules.
- **Year-pair labelling (open item):** labelled exactly as SOI does,
  `2022-2023` (returns filed in calendar 2022 matched to 2023). The period
  is 2022-01-01 to 2023-12-31; `year` for the file totals is year 2.
- **Aggregate rows:** the six header rows (US and foreign, US, same state,
  different state, foreign, non-migrants), the seven Other flows rows and
  the five foreign rows each keep a registered category and SOI's label;
  none is redistributed. `-1` in all three measures is `withheld`.
- **Two-geography fact:** recorded as
  [ADR-0008](../../decisions/0008-origin-destination-flow-facts.md). A
  county flow is admitted only when subject, origin and destination
  resolve; it is otherwise refused by side into `flow_quarantine`.
- **Route (open item):** a new resource, `/api/v1/migration-flows`, not
  under observations, because a flow has two geographies; it says
  `derived: false`. The file totals are also one-geography observations
  through `/api/v1/observations` and the glossary
  (`IRS_MIGRATION:<direction>:<category>:<measure>`).
- **Totals reconcile exactly.** In the fixtures, a county's flows, Other
  flows and foreign rows sum to the file's total migration exactly, so
  `DQ-IRS-004` uses no tolerance when nothing is withheld and requires the
  parts not to exceed the total when something is. SOI documents no
  tolerance.

## Evidence (2026-10-06, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2191 passed, including
  `tests/unit/irs_migration` (8, among them the ADR-0008 refusal test) and
  `tests/unit/api/test_migration_flows.py` (4).
- Database: `tests/integration/database/test_irs_migration_capture_replay.py`
  -- 5 passed: flows to gold with both ends resolved and unresolved
  counties refused by side; withheld categories and the publisher harvest;
  rerun and a revised file kept beside the old; `DQ-IRS-002` and
  `DQ-IRS-004` passing and then failing on a lost row, with a short row
  quarantined; the CHECK refusing a flow without an origin key, and the
  schema reapplied.
- End to end: `tests/e2e/test_irs_migration_pipeline.py` serves a file
  total through `/api/v1/observations` and top origins and destinations,
  with a withheld category, through `/api/v1/migration-flows`.
- Live: `tests/external/test_irs_migration_source_contracts.py` -- 6
  passed against www.irs.gov.
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 461 passed, 1 failed. The failure is the PEP teardown node,
  which fails on `main` too because the catalog agreement's PEP fixture
  leaves captures behind; the fix is on
  `test/catalog-agreement-fixture-residue`.
- DAG: `pytest -m "not external" tests/dags` in the scheduler container --
  155 passed; `test_dag_pipeline_execution.py` on its own, on a fresh test
  database -- 4 passed, with `irs_migration_ingest` in the orchestrated run
  (files without a reviewed fixture are served as their header alone).
- `ruff check .` clean; schema snapshot, OpenAPI contract (the new route),
  viz coverage and plan environments regenerated.

## Remaining

- Deliverable 6: "Where people came from" and "Where people went" lists in
  the People chapter with the withheld note, and the different-populations
  statement beside PEP net migration in the Change chapter. Builds on
  `feat/place-pages`.

## Checkpoint

Next pickup: branch from `feat/place-pages`, merge
`feat/irs-county-migration`, and add the migration lists.
